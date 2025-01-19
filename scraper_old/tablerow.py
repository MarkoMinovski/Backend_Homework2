class TableRow:
    def __init__(self):
        self.date = None
        self.last_trade_price = None
        self.max = None
        self.min = None
        self.avg = None
        self.percentage_change_as_decimal = None
        self.volume = None
        self.BEST_turnover_in_denars = None
        self.total_turnover_in_denars = None

    def __str__(self):
        return (
            f"TableRow(\n"
            f"  date={self.date},\n"
            f"  last_trade_price={self.last_trade_price},\n"
            f"  max={self.max},\n"
            f"  min={self.min},\n"
            f"  avg={self.avg},\n"
            f"  percentage_change_as_decimal={self.percentage_change_as_decimal},\n"
            f"  volume={self.volume},\n"
            f"  BEST_turnover_in_denars={self.BEST_turnover_in_denars},\n"
            f"  total_turnover_in_denars={self.total_turnover_in_denars}\n"
            f")"
        )