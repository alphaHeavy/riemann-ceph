[Riemann](http://riemann.io/) <3 [Ceph](http://ceph.com/)
===

Have you ever tried to monitor your Ceph cluster? Have you ever tried it... with _Riemann_?

If your curious, go ahead and build it. We won't tell anyone. Running it is simple. Boot up a Riemann instance and run this from an account and machine with the `ceph` client available:

    ./ceph-riemann --delay 10

This will run `ceph report`, parse the results and ram them into Riemann with great speed. And it will do this every 10 seconds. Forever. Providing it's listening on `127.0.0.1:5555`.

Is your glorious _Riemann_ instance running on another server or port? No problem. Toss in the optional `--server` argument and behold: your metrics will flow to the whatever host you please.

    ./ceph-riemann --delay 10 --server 74.125.239.18:5555

That's it. There are no other options, flags or any other bullsh*t. Enjoy.
