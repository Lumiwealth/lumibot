# FRED error redaction

Requests HTTPError text includes the final query URL. FRED authenticates using
an api_key query parameter, and get_snapshot formerly copied that error text
into agent-visible tool results. A failed series could therefore expose the key
even though FRED access worked for the other series.

The request boundary now replaces HTTP/transport exception messages with a
status or exception class, suppressing the original chained URL. Per-series
errors and partial successful results remain intact. No alias substitution,
credential rotation, retry expansion, or fabricated macro values are introduced.
The synthetic HTTP-400 regression fails on the old implementation and verifies
the exact credential-free error result on the corrected implementation.
