import time

"""
    Jacobson RTT estimation + Karn's algorithm.

    Terminology used here:

        ARTT = Actual Round Trip Time
        PRTT = Predicted Round Trip Time
        AD   = Actual Deviation
        PD   = Predicted Deviation
        TOT  = Timeout Time

    Jacobson:
        PRTT = alpha * old_PRTT + (1-alpha) * ARTT
        PD   = alpha * old_PD   + (1-alpha) * AD
        TOT  = PRTT + 4 * PD

    Karn:
        If timeout occurs and packet is retransmitted:
        - DO NOT use the resulting ACK to calculate ARTT
        - Double TOT
    """

class JacobsonRTTWithKarnsModification:

    # Initialization
    def __init__(self, alpha=0.875, initial_tot=1.0, min_tot=0.2, max_tot=5.0):
        self.alpha = alpha
        self.prtt = None
        self.pd = None
        self.tot = initial_tot
        self.min_tot = min_tot
        self.max_tot = max_tot

    def update(self, artt):
        if self.prtt is None:
            self.prtt = artt
            self.pd = artt/2
        else:
            ad = abs(self.prtt - artt)
            self.prtt = self.alpha*self.prtt + (1-self.alpha)*artt
            self.pd = self.alpha*self.pd + (1-self.alpha)*ad
        
        self.tot = 4*self.pd + self.prtt
        self.tot = max(self.min_tot, min(self.tot,self.max_tot))

    def timeout(self):
        self.tot = self.tot * 2
        self.tot = min(self.tot, self.max_tot)

    def getTimeout(self):
            return self.tot

    def __str__(self):

        if self.prtt is None:
            return (
                f"PRTT = None, "
                f"PD = None, "
                f"TOT = {self.tot:.3f}s"
            )

        return (
            f"PRTT = {self.prtt:.3f}s, "
            f"PD = {self.pd:.3f}s, "
            f"TOT = {self.tot:.3f}s"
        )

    def now():
        return time.monotonic()
    