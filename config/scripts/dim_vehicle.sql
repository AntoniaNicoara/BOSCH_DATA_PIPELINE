SELECT
    v.make,
    v.model,
    CAST(v.year AS INT) as year,
    v.VClass as vehicle_class,
    v.fuelType1 as fuel_type,
    v.engId as engine_id,
    e.ghgScore as emissions_score,
    s.OVERALL_STARS as safety_stars
FROM dev.silver.vehicles_silver v
LEFT JOIN dev.silver.fueleconomy_emissions_silver e
    ON v.id = e.id
LEFT JOIN dev.silver.nhtsa_safercar_silver s
    ON v.make = s.MAKE
   AND v.model = s.MODEL
   AND v.year = s.MODEL_YR