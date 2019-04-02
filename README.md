# NkSERVER OT

Opentracing plugin for NkSERVER

Each package or service based on nkserver can activate opentracing capabilities by inserting this plugin.

Service must define the key 'opentrace_rules' (see [nkserver_ot_rules](src/nkserver_ot_rules.erl)), with a string implementing the filtering rules (created by FIFO's [_otters_](https://github.com/project-fifo/otters) project). These rules are compiled on plugin start and at each update, and applied to generated all spans to decide if they must be sent or discarded.

There are a reach set of ways to generate spans (see [nkserver_ot](src/nkserver_ot.erl)).

Spans, we finished, are sent to an aggregator _gen_server_ process, that sends them in batches to the Zipkin-compatible server.

This project is based on [_otter_](https://github.com/Bluehouse-Technology/otter) and [_otters_](https://github.com/project-fifo/otters) projects.  

Application-specific keys configure the zipking url and send interval (see [shell.config](config/shell.config) for a sample). 