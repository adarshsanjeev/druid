select
countryName, cityName,
row_number() over (order by cityName) as c1
from wikipedia
where countryName in ('Austria', 'Republic of Korea')
group by countryName, cityName, channel
