WITH airports_reorder AS (
    SELECT faa
    	   ,region
    	   ,country
           ,city
           ,name
           FROM {{ref('staging_airports')}}
)
SELECT * FROM airports_reorder