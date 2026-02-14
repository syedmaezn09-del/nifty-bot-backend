"""
DhanHQ Trading Bot - Main Application
Production-ready trading bot with FastAPI backend
"""

import asyncio
import json
import logging
import os
import signal
import sys
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from decimal import Decimal
import uuid

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Depends, BackgroundTasks, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import JSONResponse
import uvicorn
from pydantic import BaseModel, Field, validator
import structlog

# Import custom modules
from core.config import Settings, get_settings
from core.dhan_client import DhanClient
from core.risk_manager import RiskManager
from core.order_executor import OrderExecutor
from core.market_data import MarketDataManager
from core.state_manager import StateManager
from core.auth import verify_token, create_access_token
from models.trading import (
    TradeRequest, OrderResponse, Position, MarketQuote,
    WebSocketMessage, ConnectionStatus, AccountInfo
)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

logger = structlog.get_logger()

# Global state
app_state = {
    "dhan_client": None,
    "risk_manager": None,
    "order_executor": None,
    "market_data": None,
    "state_manager": None,
    "websocket_connections": [],
    "is_running": False,
    "start_time": None,
    "shutdown_event": asyncio.Event()
}

# Pydantic models for API requests/responses
class LoginRequest(BaseModel):
    client_id: str
    access_token: str

class PlaceOrderRequest(BaseModel):
    security_id: str
    exchange_segment: str = "NSE_FNO"
    transaction_type: str  # BUY or SELL
    quantity: int
    order_type: str = "MARKET"  # MARKET, LIMIT, SL, SL-M
    product_type: str = "INTRADAY"  # INTRADAY, CNC, MARGIN, CO, BO
    price: float = 0.0
    trigger_price: float = 0.0
    tag: Optional[str] = None

class ModifyOrderRequest(BaseModel):
    order_id: str
    quantity: Optional[int] = None
    price: Optional[float] = None
    trigger_price: Optional[float] = None

class SuperOrderRequest(BaseModel):
    security_id: str
    exchange_segment: str = "NSE_FNO"
    transaction_type: str
    quantity: int
    price: float
    trigger_price: float = 0.0
    target_price: float
    stop_loss_price: float
    trailing_stop_loss: float = 0.0
    tag: Optional[str] = None

# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager"""
    logger.info("Starting DhanHQ Trading Bot...")
    app_state["start_time"] = datetime.utcnow()
    app_state["is_running"] = True
    
    # Initialize state manager
    app_state["state_manager"] = StateManager()
    await app_state["state_manager"].initialize()
    
    # Setup signal handlers
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
    
    logger.info("Trading bot started successfully")
    yield
    
    # Shutdown
    await shutdown()

async def shutdown():
    """Graceful shutdown"""
    if app_state["shutdown_event"].is_set():
        return
    
    logger.info("Shutting down trading bot...")
    app_state["shutdown_event"].set()
    app_state["is_running"] = False
    
    # Close WebSocket connections
    for ws in app_state["websocket_connections"]:
        try:
            await ws.close()
        except:
            pass
    
    # Close market data connection
    if app_state["market_data"]:
        await app_state["market_data"].close()
    
    # Save state
    if app_state["state_manager"]:
        await app_state["state_manager"].save_state()
    
    logger.info("Shutdown complete")

# Create FastAPI app
app = FastAPI(
    title="DhanHQ Trading Bot API",
    description="Production-ready trading bot with DhanHQ integration",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBearer()

# Dependency to get current user
async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    token = credentials.credentials
    payload = verify_token(token)
    if not payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload

# ============ API Routes ============

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "DhanHQ Trading Bot API",
        "version": "1.0.0",
        "status": "running" if app_state["is_running"] else "stopped",
        "uptime": (datetime.utcnow() - app_state["start_time"]).total_seconds() if app_state["start_time"] else 0
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "is_running": app_state["is_running"],
        "dhan_connected": app_state["dhan_client"] is not None and app_state["dhan_client"].is_connected(),
        "websocket_clients": len(app_state["websocket_connections"]),
        "uptime_seconds": (datetime.utcnow() - app_state["start_time"]).total_seconds() if app_state["start_time"] else 0
    }

@app.post("/auth/login")
async def login(credentials: LoginRequest):
    """Login with DhanHQ credentials"""
    try:
        # Initialize Dhan client
        dhan_client = DhanClient(credentials.client_id, credentials.access_token)
        
        # Test connection by getting fund limits
        fund_limits = await dhan_client.get_fund_limits()
        
        if fund_limits:
            # Store client in app state
            app_state["dhan_client"] = dhan_client
            
            # Initialize other components
            app_state["risk_manager"] = RiskManager(dhan_client)
            app_state["order_executor"] = OrderExecutor(dhan_client)
            app_state["market_data"] = MarketDataManager(dhan_client)
            
            # Start market data
            await app_state["market_data"].start()
            
            # Create JWT token
            token = create_access_token({
                "client_id": credentials.client_id,
                "sub": credentials.client_id
            })
            
            logger.info("Login successful", client_id=credentials.client_id)
            
            return {
                "success": True,
                "message": "Login successful",
                "access_token": token,
                "token_type": "bearer",
                "fund_limits": fund_limits
            }
        else:
            raise HTTPException(status_code=401, detail="Invalid credentials")
    
    except Exception as e:
        logger.error("Login failed", error=str(e))
        raise HTTPException(status_code=401, detail=f"Login failed: {str(e)}")

@app.post("/auth/logout")
async def logout(user: dict = Depends(get_current_user)):
    """Logout and cleanup"""
    if app_state["market_data"]:
        await app_state["market_data"].close()
    
    app_state["dhan_client"] = None
    app_state["risk_manager"] = None
    app_state["order_executor"] = None
    app_state["market_data"] = None
    
    return {"success": True, "message": "Logged out successfully"}

# ============ Trading Endpoints ============

@app.get("/account/info")
async def get_account_info(user: dict = Depends(get_current_user)):
    """Get account information"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        fund_limits = await app_state["dhan_client"].get_fund_limits()
        return {"success": True, "data": fund_limits}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/account/positions")
async def get_positions(user: dict = Depends(get_current_user)):
    """Get current positions"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        positions = await app_state["dhan_client"].get_positions()
        return {"success": True, "data": positions}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/account/orders")
async def get_orders(user: dict = Depends(get_current_user)):
    """Get order history"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        orders = await app_state["dhan_client"].get_order_list()
        return {"success": True, "data": orders}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/account/trades")
async def get_trades(user: dict = Depends(get_current_user)):
    """Get trade history"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        trades = await app_state["dhan_client"].get_trade_history()
        return {"success": True, "data": trades}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/place")
async def place_order(order: PlaceOrderRequest, user: dict = Depends(get_current_user)):
    """Place a new order"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Check risk limits
        risk_check = await app_state["risk_manager"].check_order_risk(order)
        if not risk_check["allowed"]:
            return {"success": False, "error": risk_check["reason"]}
        
        # Place order
        response = await app_state["dhan_client"].place_order(
            security_id=order.security_id,
            exchange_segment=order.exchange_segment,
            transaction_type=order.transaction_type,
            quantity=order.quantity,
            order_type=order.order_type,
            product_type=order.product_type,
            price=order.price,
            trigger_price=order.trigger_price,
            tag=order.tag
        )
        
        logger.info("Order placed", 
                   order_id=response.get("orderId"),
                   symbol=order.security_id,
                   quantity=order.quantity)
        
        return {"success": True, "data": response}
    
    except Exception as e:
        logger.error("Order placement failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/modify")
async def modify_order(order: ModifyOrderRequest, user: dict = Depends(get_current_user)):
    """Modify an existing order"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        response = await app_state["dhan_client"].modify_order(
            order_id=order.order_id,
            quantity=order.quantity,
            price=order.price,
            trigger_price=order.trigger_price
        )
        return {"success": True, "data": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/cancel/{order_id}")
async def cancel_order(order_id: str, user: dict = Depends(get_current_user)):
    """Cancel an order"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        response = await app_state["dhan_client"].cancel_order(order_id)
        return {"success": True, "data": response}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/orders/super")
async def place_super_order(order: SuperOrderRequest, user: dict = Depends(get_current_user)):
    """Place a Super Order (Bracket Order with OCO)"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        response = await app_state["dhan_client"].place_super_order(
            security_id=order.security_id,
            exchange_segment=order.exchange_segment,
            transaction_type=order.transaction_type,
            quantity=order.quantity,
            price=order.price,
            trigger_price=order.trigger_price,
            target_price=order.target_price,
            stop_loss_price=order.stop_loss_price,
            trailing_stop_loss=order.trailing_stop_loss,
            tag=order.tag
        )
        
        logger.info("Super order placed",
                   order_id=response.get("orderId"),
                   symbol=order.security_id,
                   target=order.target_price,
                   stop_loss=order.stop_loss_price)
        
        return {"success": True, "data": response}
    
    except Exception as e:
        logger.error("Super order placement failed", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

# ============ Market Data Endpoints ============

@app.get("/market/quote/{security_id}")
async def get_market_quote(security_id: str, exchange: str = "NSE_FNO", user: dict = Depends(get_current_user)):
    """Get market quote for a security"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        quote = await app_state["dhan_client"].get_market_quote(security_id, exchange)
        return {"success": True, "data": quote}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/market/option-chain/{underlying}")
async def get_option_chain(underlying: str, exchange: str = "NSE_FNO", user: dict = Depends(get_current_user)):
    """Get option chain for underlying"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        chain = await app_state["dhan_client"].get_option_chain(underlying, exchange)
        return {"success": True, "data": chain}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/market/historical/{security_id}")
async def get_historical_data(
    security_id: str,
    exchange: str = "NSE_FNO",
    from_date: str = None,
    to_date: str = None,
    interval: str = "1",
    user: dict = Depends(get_current_user)
):
    """Get historical candle data"""
    if not app_state["dhan_client"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Default to last 30 days if dates not provided
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            from_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        data = await app_state["dhan_client"].get_historical_data(
            security_id, exchange, from_date, to_date, interval
        )
        return {"success": True, "data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/market/subscribe")
async def subscribe_instruments(instruments: List[dict], user: dict = Depends(get_current_user)):
    """Subscribe to market data for instruments"""
    if not app_state["market_data"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        await app_state["market_data"].subscribe(instruments)
        return {"success": True, "message": f"Subscribed to {len(instruments)} instruments"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============ Risk Management Endpoints ============

@app.get("/risk/status")
async def get_risk_status(user: dict = Depends(get_current_user)):
    """Get current risk status"""
    if not app_state["risk_manager"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        status = await app_state["risk_manager"].get_risk_status()
        return {"success": True, "data": status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/risk/limits")
async def update_risk_limits(limits: dict, user: dict = Depends(get_current_user)):
    """Update risk limits"""
    if not app_state["risk_manager"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        await app_state["risk_manager"].update_limits(limits)
        return {"success": True, "message": "Risk limits updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/risk/emergency-exit")
async def emergency_exit(user: dict = Depends(get_current_user)):
    """Emergency exit - close all positions"""
    if not app_state["order_executor"]:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        result = await app_state["order_executor"].emergency_exit()
        logger.warning("Emergency exit executed", result=result)
        return {"success": True, "data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ============ WebSocket Endpoint ============

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket for real-time market data and updates"""
    await websocket.accept()
    app_state["websocket_connections"].append(websocket)
    
    try:
        # Send initial connection success
        await websocket.send_json({
            "type": "connection",
            "status": "connected",
            "timestamp": datetime.utcnow().isoformat()
        })
        
        while app_state["is_running"]:
            try:
                # Receive message from client
                message = await asyncio.wait_for(
                    websocket.receive_json(),
                    timeout=1.0
                )
                
                # Handle different message types
                msg_type = message.get("type")
                
                if msg_type == "subscribe":
                    instruments = message.get("instruments", [])
                    if app_state["market_data"]:
                        await app_state["market_data"].subscribe(instruments)
                    await websocket.send_json({
                        "type": "subscribed",
                        "instruments": instruments
                    })
                
                elif msg_type == "unsubscribe":
                    instruments = message.get("instruments", [])
                    if app_state["market_data"]:
                        await app_state["market_data"].unsubscribe(instruments)
                    await websocket.send_json({
                        "type": "unsubscribed",
                        "instruments": instruments
                    })
                
                elif msg_type == "ping":
                    await websocket.send_json({"type": "pong"})
                
            except asyncio.TimeoutError:
                # No message received, continue to broadcast updates
                pass
            
            # Broadcast market data updates if available
            if app_state["market_data"]:
                updates = await app_state["market_data"].get_updates()
                if updates:
                    await websocket.send_json({
                        "type": "market_data",
                        "data": updates
                    })
    
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error("WebSocket error", error=str(e))
    finally:
        if websocket in app_state["websocket_connections"]:
            app_state["websocket_connections"].remove(websocket)

# ============ Background Tasks ============

async def monitor_positions():
    """Background task to monitor positions and stop-losses"""
    while app_state["is_running"]:
        try:
            if app_state["order_executor"]:
                await app_state["order_executor"].monitor_positions()
            await asyncio.sleep(2)  # Check every 2 seconds
        except Exception as e:
            logger.error("Position monitoring error", error=str(e))
            await asyncio.sleep(5)

async def sync_with_broker():
    """Background task to sync with broker"""
    while app_state["is_running"]:
        try:
            if app_state["state_manager"] and app_state["dhan_client"]:
                await app_state["state_manager"].sync_with_broker(app_state["dhan_client"])
            await asyncio.sleep(30)  # Sync every 30 seconds
        except Exception as e:
            logger.error("Broker sync error", error=str(e))
            await asyncio.sleep(60)

# Start background tasks
@app.on_event("startup")
async def start_background_tasks():
    asyncio.create_task(monitor_positions())
    asyncio.create_task(sync_with_broker())

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        workers=1,
        log_level="info"
    )
