# Expired Key Repository

An expired key repository is basically a key-value store where the keys have time-to-live and expire once the<br>
time elapses, after which it should be automatically removed from the repository along with its value.<br><br>

Example usecases:

- session management in web browsers by storing user's session data which expires after a period of inactivity.
- caching frequently accessed data with expiration dates for periodic refreshes
- rate limiting by tracking a user's rate limits with keys that expire after a time duration
- store for auth tokens, OTP codes and password reset codes that should be valid for a preset duration
- deduplication in an asynchronous message-oriented middleware using at-least-once delivery

This implementation was inspired by the last use case above. I was implementing pub/sub messaging for a<br>project
at work using [Watermill](https://watermill.io/docs/getting-started/). One of the configurations you can specify for
message delivery guarantees is<br>
`at-least-once delivery`. The publisher will try sending the message until it gets an acknowledgement from<br>
the subscriber.<br>
This is good for reliability but there's always likelihood that the message will be delivered more than once<br>
to the subscriber. If duplicates are not a big deal in your application then that's no problem, but in most<br>
cases you want the application to be idempotent and this where things get interesting. The approach used by<br>
Watermill for deduplication is by using a key-value store with expiring keys. The underlying key-value store<br>
used is a hash map with a mutex for concurrency control. According to the Watermill docs, the downside of<br>
this implementation is that:
<i>

```
The state **cannot be shared or synchronized between instances** by design for performance.
```

</i>
In this implementation I will try building a KV repository with expiring keys backed by an in-memory KV<br>
store, specifically Redis but you can swap it out for your preferred choice

## Key Features
- Time-based validity for the keys
- Automatic cleanup of keys after expiry
- Lazy expiration cleanup strategy
