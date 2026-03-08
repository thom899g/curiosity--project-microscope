"""
Sandbox Simulation Engine
Simulates trade execution in historical and adversarial conditions
"""
import os
import json
import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime

import pandas as pd
import numpy as np
from web3 import Web3, HTTPProvider
from web3.middleware import geth_poa_middleware
from web3.exceptions import ContractLogicError, TransactionNotFound
from eth_account import Account
from eth_typing import HexStr

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


@dataclass
class SimulationConfig:
    """Configuration for simulation environment"""
    base_rpc_url: str = "https://mainnet.base.org"
    local_rpc_url: str = "http://localhost:8545"
    fork_block_number: Optional[int] = None
    gas_price_multiplier: float = 1.5
    max_slippage_bps: int = 200  # 2%
    mev_bot_address: str = "0x0000000000000000000000000000000000000000"
    simulation_timeout: int = 30  # seconds


@dataclass
class TradeHypothesis:
    """A trade hypothesis to be validated"""
    tx_hash: str
    block_number: int
    trader: str
    token_in: str
    token_out: str
    amount_in: Decimal
    expected_profit: Decimal
    dex_pool: str
    dex_type: str = "uniswap_v3"
    timestamp: int = 0


@dataclass
class SimulationResult:
    """Results of a trade simulation"""
    hypothesis_id: str
    simulated_profit: Decimal
    actual_profit: Decimal
    success: bool
    gas_used: int
    gas_cost_eth: Decimal
    slippage_bps: int
    mev_risk_score: float  # 0-1, higher = more risky
    execution_time_ms: int
    failure_reason: Optional[str] = None
    adversarial_survived: bool = True
    simulation_timestamp: int = 0


class SandboxSimulator:
    """Main simulation engine for validating trade hypotheses"""
    
    def __init__(self, config: SimulationConfig):
        self.config = config
        self.web3_mainnet = None
        self.web3_local = None
        self._init_web3_clients()
        self.simulation_history: List[SimulationResult] = []
        
    def _init_web3_clients(self) -> None:
        """Initialize Web3 connections with error handling"""
        try:
            # Mainnet connection (read-only)
            self.web3_mainnet = Web3(HTTPProvider(self.config.base_rpc_url))
            self.web3_mainnet.middleware_onion.inject(geth_poa_middleware, layer=0)
            
            if not self.web3_mainnet.is_connected():
                raise ConnectionError(f"Cannot connect to Base RPC: {self.config.base_rpc_url}")
                
            logger.info(f"Connected to Base mainnet. Chain ID: {self.web3_mainnet.eth.chain_id}")
            
            # Local forked chain connection
            self.web3_local = Web3(HTTPProvider(self.config.local_rpc_url))
            
            if not self.web3_local.is_connected():
                logger.warning("Local node not available, running in limited mode")
                
        except Exception as e:
            logger.error(f"Failed to initialize Web3 clients: {e}")
            raise
    
    def fork_chain_at_block(self, block_number: int) -> bool:
        """
        Fork the chain at specific block number
        Returns: True if successful
        """
        try:
            if not self.web3_local or not self.web3_local.is_connected():
                logger.error("Local node not available for forking")
                return False
            
            # In production, this would use Hardhat or Ganache forking
            # For now, we simulate by caching block state
            logger.info(f"Forking chain at block {block_number}")
            
            # Store current block info for simulation context
            self.current_fork_block = block_number
            self.fork_timestamp = int(time.time())
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to fork chain at block {block_number}: {e}")
            return False
    
    def replay_historical_block(self, block_number: int) -> Dict[str, Any]:
        """
        Replay a historical block and extract all trades
        Returns: Dictionary with block data and extracted trades
        """
        try:
            if not self.web3_mainnet.is_connected():
                raise ConnectionError("Mainnet connection not available")
            
            logger.info(f"Replaying block {block_number}")
            
            # Get block with transactions
            block = self.web3_mainnet.eth.get_block(block_number, full_transactions=True)
            
            trades = []
            for tx in block.transactions:
                # Check if transaction is a DEX swap
                if self._is_swap_transaction(tx):
                    trade_data = self._extract_trade_data(tx, block_number)
                    if trade_data:
                        trades.append(trade_data)
            
            return {
                "block_number": block_number,
                "timestamp": block.timestamp,
                "total_transactions": len(block.transactions),
                "swap_transactions": len(trades),
                "trades": trades,
                "base_fee_per_gas": block.baseFeePerGas if hasattr(block, 'baseFeePerGas') else None
            }
            
        except Exception as e:
            logger.error(f"Failed to replay block {block_number}: {e}")
            return {"error": str(e), "block_number": block_number}
    
    def _is_swap_transaction(self, tx) -> bool:
        """Check if transaction is a DEX swap"""
        # Common DEX routers on Base
        dex_routers = {
            "0x4752ba5dbc23f44d87826276bf6fd6b1c372ad24",  # BaseSwap Router
            "0x327df1e6de05895d2ab08513aadd9313fe505d86",  // Aerodrome Router
            "0x2626664c2603336e57b271c5c0b26f421741e481",  // Uniswap V3 Router
        }
        
        if not tx.get('to'):
            return False
        
        return tx['to'].lower() in dex_routers or tx.get('input', '').startswith('0x')
    
    def _extract_trade_data(self, tx, block_number: int) -> Optional[Dict[str, Any]]:
        """Extract trade data from transaction"""
        try:
            # Get transaction receipt
            receipt = self.web3_mainnet.eth.get_transaction_receipt(tx.hash)
            
            # Parse swap events from logs
            swap_events = self._parse_swap_events(receipt.logs)
            
            if not swap_events:
                return None
            
            # Calculate profit (simplified - would need price feeds in production)
            # This is a placeholder for actual profit calculation
            estimated_profit = Decimal('0')
            
            return {
                "tx_hash": tx.hash.hex(),
                "trader": tx['from'],
                "block_number": block_number,
                "timestamp": self.web3_mainnet.eth.get_block(block_number).timestamp,
                "gas_used": receipt.gasUsed,
                "gas_price": tx.get('gasPrice', tx.get('maxFeePerGas', 0)),
                "swap_events": swap_events,
                "estimated_profit_eth": float(estimated_profit),
                "success": receipt.status == 1
            }
            
        except Exception as e:
            logger.warning(f"Failed to extract trade data from tx {tx.hash.hex()}: {e}")
            return None
    
    def _parse_swap_events(self, logs) -> List[Dict[str, Any]]:
        """Parse swap events from transaction logs"""
        swap_events = []
        
        # Common swap event signatures
        swap_signatures = [
            "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822",  // Uniswap V2
            "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67",  // Uniswap V3
        ]
        
        for log in logs:
            if len(log.topics) > 0 and log.topics[0].hex() in swap_signatures:
                try:
                    swap_events.append({
                        "contract": log.address,
                        "topics": [t.hex() for t in log.topics],
                        "data": log.data.hex(),
                        "block_number": log.blockNumber
                    })
                except:
                    continue
        
        return swap_events
    
    def simulate_trade(self, hypothesis: TradeHypothesis) -> SimulationResult:
        """
        Simulate a trade hypothesis against historical conditions
        """
        start_time = time.time()
        simulation_id = f"sim_{hypothesis.tx_hash[:8]}_{int(start_time)}"
        
        try:
            # Fork chain at block before the trade
            fork_block = hypothesis.block_number - 1
            if not self.fork_chain_at_block(fork_block):
                return SimulationResult(
                    hypothesis_id=simulation_id,
                    simulated_profit=Decimal('0'),
                    actual_profit=Decimal('0'),
                    success=False,
                    gas_used=0,
                    gas_cost_eth=Decimal('0'),
                    slippage_bps=0,
                    mev_risk_score=1.0,
                    execution_time_ms=int((time.time() - start_time) * 1000),
                    failure_reason="Chain fork failed"
                )
            
            # Calculate simulated profit
            simulated_profit = self._calculate_simulated_profit(hypothesis)
            
            # Test against adversarial conditions
            mev_risk_score = self._simulate_mev_attack(hypothesis)
            
            # Calculate gas costs
            gas_used = 150000  # Estimated gas for a simple swap
            gas_price = self._get_gas_price_at_block(hypothesis.block_number)
            gas_cost_eth = Decimal(str(gas_used * gas_price / 1e18))
            
            # Calculate actual profit (simulated profit minus costs)
            actual_profit = simulated_profit - gas_cost_eth
            
            # Determine success
            success = actual_profit > Decimal('0') and mev_risk_score < 0.7
            
            result = SimulationResult(
                hypothesis_id=simulation_id,
                simulated_profit=simulated_profit,
                actual_profit=actual_profit,
                success=success,
                gas_used=gas_used,
                gas_cost_eth=gas_cost_eth,
                slippage_bps=self._calculate_slippage(hypothesis),
                mev_risk_score=mev_risk_score,
                execution_time_ms=int((time.time() - start_time) * 1000),
                adversarial_survived=mev_risk_score < 0.5
            )
            
            self.simulation_history.append(result)
            logger.info(f"Simulation {simulation_id} completed: success={success}, profit={actual_profit}")
            
            return result
            
        except Exception as e:
            logger.error(f"Simulation failed for {simulation_id}: {e}")
            return SimulationResult(
                hypothesis_id=simulation_id,
                simulated_profit=Decimal('0'),
                actual_profit=Decimal('0'),
                success=False,
                gas_used=0,
                gas_cost_eth=Decimal('0'),
                slippage_bps=0,
                mev_risk_score=1.0,
                execution_time_ms=int((time.time() - start_time) * 1000),
                failure_reason=str(e)
            )
    
    def _calculate_simulated_profit(self, hypothesis: TradeHypothesis) -> Decimal:
        """Calculate simulated profit for a trade hypothesis"""
        # This is a simplified calculation
        # In production, would query DEX pools and calculate exact output
        
        # Placeholder: return the expected profit with some randomness
        # to simulate real-world conditions
        noise = Decimal(str(np.random.normal(0, 0.01)))  # 1% noise
        return hypothesis.expected_profit * (Decimal('1') + noise)
    
    def _simulate_mev_attack(self, hypothesis: TradeHypothesis) -> float:
        """Simulate MEV attack against the trade"""
        # Check if trade is vulnerable to sandwich attacks
        risk_factors = []
        
        # Factor 1: Transaction value (higher = more attractive)
        if hypothesis.amount_in > Decimal('100'):
            risk_factors.append(0.3)
        
        # Factor 2: Slippage tolerance
        if self._calculate_slippage(hypothesis) > 100:  # >1%
            risk_factors.append(0.4)
        
        # Factor 3: Pool liquidity (lower = more vulnerable)
        # Would need actual pool data in production
        risk_factors.append(0.2)
        
        # Factor 4: Time of day (MEV bots more active during high volume)
        hour = datetime.fromtimestamp(hypothesis.timestamp).hour
        if 14 <= hour <= 22:  # 2PM-10PM UTC
            risk_factors.append(0.1)
        
        return float(np.mean(risk_factors)) if risk_factors else 0.0
    
    def _calculate_slippage(self, hypothesis: TradeHypothesis) -> int:
        """Calculate estimated slippage in basis points"""
        # Simplified calculation
        base_slippage = 50  # 0.5% base slippage
        
        # Adjust based on trade size
        size_factor = min(float(hypothesis.amount_in) / 500, 1.0)  # Normalize to $500
        adjusted_slippage = int(base_slippage * (1 + size_factor))
        
        return min(adjusted_slippage, self.config.max_slippage_bps)
    
    def _get_gas_price_at_block(self, block_number: int) -> int:
        """Get historical gas price at specific block"""
        try:
            block = self.web3_mainnet.eth.get_block(block_number)
            if hasattr(block, 'baseFeePerGas') and block.baseFeePerGas:
                return int(block.baseFeePerGas * self.config.gas_price_multiplier)
        except:
            pass
        
        # Fallback: current gas price
        return int(self.web3_mainnet.eth.gas_price * self.config.gas_price_multiplier)
    
    def batch_simulate(self, hypotheses: List[TradeHypothesis]) -> List[SimulationResult]:
        """Simulate multiple trade hypotheses"""
        results = []
        for i, hypothesis in enumerate(hypotheses):
            logger.info(f"Simulating hypothesis {i+1}/{