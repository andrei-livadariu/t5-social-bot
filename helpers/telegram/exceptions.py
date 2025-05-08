# This class is used for exceptions that can be directly displayed to the user
class UserFriendlyError(Exception):
    pass


# This class indicates an issue with the syntax of a command
class CommandSyntaxError(Exception):
    pass

# Used when we get a message from a user without a telegram username
class MissingUsernameError(UserFriendlyError):
    def __init__(self, msg="I don't really know who you are - to use this feature you first need to create a username in Telegram.", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)

# Used when we can't find a user in our community database
class UserNotFoundError(UserFriendlyError):
    def __init__(self, msg="Sorry, but this feature is only available to community members. Talk to @roblever about becoming one!", *args, **kwargs):
        super().__init__(msg, *args, **kwargs)
