# Deduplication

Deduplication is a technique of removing duplicate copies of repeating data. It is useful in many different contexts such as:

- in storage systems to reduce storage requirement needs
- in network transfer to reduce the amount of bytes sent over the network
- in message-oriented systems to avoid processing the same message twice
- in targeted ads systems to avoid showing the user the same ad
- in product recommendation systems to avoid showing the user the same product

Deduplication systems can be categorized according to a number of criteria:

<ol type="i">
	<li>
		post-process deduplication: new data is first stored on device then later a process analyzes the data looking for duplicates.
	</li>
	<li>
		inline deduplication: done as data is incoming on the device to look for and eliminate duplicates.
	</li>
	<li>
		target deduplication: deduplication is done where the data is stored/processed.
	</li>
	<li>
		source deduplication: deduplication is done where the data is created or originating.
	</li>
</ol>

I implemented three different approaches to deduplication, each with it's own benefits. They are:

<ol type="i">
	<li>
		Expiring key repository deduplicator.
	</li>
	<li>
		Bloom filter deduplicator.
	</li>
	<li>
		Cuckoo filter deduplicator.
	</li>
</ol>
