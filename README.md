# Vidchyneno

Redis-dumper on steroids.

## Usage - Highlevel

1. Have a list of domains to target

2. Find which AS they all belong to

3. Extract CIDR blocks for the AS responsible for hosting the domain

4. Scan all CIDR blocks for Redis port

5. See if we can connect to it

6. See if it's unauthenticated

7. Dump all the contents we can get our hands on
