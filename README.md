# Expired Key Repository

An expired key repository is basically a key-value store where the keys have time-to-live and expire once the time elapses,<br>
after which it should be automatically removed from the repository along with its value.<br><br>
Example usecases:

- session management in web browsers by storing user's session data which expires after a period of inactivity.
- caching frequently accessed data with expiration dates for periodic refreshes
- rate limiting by tracking a user's rate limits with keys that expire after a time duration
- store for auth tokens, OTP codes and password reset codes that should be valid for a preset duration
- deduplication in an asynchronous message-oriented middleware using at-least-once delivery

The last example above is the motivation for this implementation.
