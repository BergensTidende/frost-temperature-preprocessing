# Frost Data Preprocessing

1. Initally, 1323 available time series were fetched with the element `mean(air_temperature) P1Y` (see [api](https://frost.met.no/api.html)). The scope was limited to 1272 time series from the SensorSystem dataset. These time series came from 966 unique sourceIds.
2. We then successfully fetched observations from 849 sources (i.e. weather stations).
3. The meta data (coordinates, etc.) from 311 stations could successfully be requested.
4. We then filtered the remaining 311 time series to a dataset containing 54 stations with the following criteria:
  - only include data from stations that have recorded data up to 2018 (311 - 89 = 222 stations)
  - only include data from stations that have at least 20 years of recordings (222 - 123  = 99 stations)
  - only include data from stations that don't have a time gap in the recordings that is bigger than 3 years (99 - 45 = 54 stations)
