class Elo:
    def __init__(self, k: float, base_rating: float = 400.0):
        self.k = k
        self.base_rating = base_rating
        self._exponent_base = 10.0

    def game_over(self, winner_rating: float, loser_rating: float) -> (float, float):
        result = self._expect_result(winner_rating, loser_rating)

        new_winner_rating = winner_rating + self.k * (1 - result)
        new_winner_rating = round(new_winner_rating, 2)

        new_loser_rating = loser_rating + self.k * (0 - (1 - result))
        new_loser_rating = round(new_loser_rating, 2)

        return new_winner_rating, new_loser_rating

    def _expect_result(self, p1_rating: float, p2_rating: float) -> float:
        exp = (p2_rating - p1_rating) / self.base_rating
        return 1 / ((self._exponent_base ** exp) + 1)