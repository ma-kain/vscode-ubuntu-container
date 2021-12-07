-- SQLite
select i.Indices, i.Date, i.Close, p."P/E" PE, (i.Close / p."P/E") Earning 
from HISTORICALINDICES i, HISTORICAL_PEPB p
WHERE i.Indices = p.Indices AND i.Date = p.Date
AND i.Indices = 'NIFTY INFRA';


SELECT Indices, count(1) from HISTORICAL_PEPB
GROUP by Indices;
