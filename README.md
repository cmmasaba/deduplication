# Expired Key Repository

An expired key repository is a store for key-value pairs. The key-value pairs are only valid for a specified time duration, after which they are automatically cleared out<br>

Example usecases:

- session management in web browsers by storing user's session data which expires after a period of inactivity.
- caching frequently accessed data with expiration dates for periodic refreshes
- rate limiting by tracking a user's rate limits with keys that expire after a time duration
- store for auth tokens, OTP codes and password reset codes that should be valid for a preset duration
- deduplication in an asynchronous message-oriented middleware using at-least-once delivery

This implementation was inspired by the last use case above. For example in pub/sub, the publisher sends messages until the subscriber acknowleges them. To maintain idempotency, the deduplicator drops messages it marks as duplicates within a specified time window.
In my implementation the repository is backed by Redis which has good benefits:

- highly efficient for lookup operations
- automatic cleanup of keys afte expiry
- supports distributed workloads
