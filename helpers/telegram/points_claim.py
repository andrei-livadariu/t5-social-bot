from telegram import InlineKeyboardMarkup, InlineKeyboardButton

from helpers.business_logic.points import Points

class PointsClaim:
    def __init__(self, points: Points):
        self._points = points

    def keyboard(self, text: str = None) -> InlineKeyboardMarkup:
        text = text or f"Claim your point{self._points.plural}!"

        return InlineKeyboardMarkup([
            [
                InlineKeyboardButton(text, callback_data=f"points/claim/{self._points}"),
            ]
        ])
