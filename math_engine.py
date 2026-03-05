import math

class MiningROIAnalytics:
    """
    Математическое ядро для расчета финансовой эффективности PoW-майнинга.
    Включает учет динамической сложности и амортизации оборудования.
    """
    
    def __init__(self, difficulty, block_reward, network_hashrate):
        self.difficulty = difficulty
        self.block_reward = block_reward
        self.network_hashrate = network_hashrate

    def calculate_daily_income(self, device_hashrate, coin_price):
        """Расчет теоретической доходности в сутки без учета комиссий."""
        # Упрощенная формула для демонстрации логики
        income_crypto = (device_hashrate * self.block_reward * 86400) / (self.difficulty * 2**32)
        return income_crypto * coin_price

    def estimate_payback_period(self, price_usd, daily_profit, electricity_cost_kwh, power_watts):
        """Расчет окупаемости (ROI) с учетом операционных расходов (OPEX)."""
        daily_expenses = (power_watts / 1000) * 24 * electricity_cost_kwh
        net_profit = daily_profit - daily_expenses
        
        if net_profit <= 0:
            return float('inf')
        
        return price_usd / net_profit
