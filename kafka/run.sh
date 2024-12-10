#!/bin/bash

# ---------------------------------------------------------------------------- #
#                                   Producers                                  #
# ---------------------------------------------------------------------------- #

python3 ./financial_data_poll_producer.py       companies.json  60 &
python3 ./financial_data_stream_producer.py     companies.json &
python3 ./currency_data_poll_producer.py        currencies.json 10 &

# ---------------------------------------------------------------------------- #
#                                   Consumers                                  #
# ---------------------------------------------------------------------------- #

python3 ./financial_data_consumer.py &

# ---------------------------------------------------------------------------- #
wait
