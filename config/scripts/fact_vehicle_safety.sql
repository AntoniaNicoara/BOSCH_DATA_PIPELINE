SELECT
    CONCAT(v.make, '_', v.model, '_', v.year) as vehicle_key,
    v.make,
    v.model,
    v.year,
    v.engId as engine_id,
    e.ghgScore as emissions_score,
    e.co2TailpipeGpm as co2,
    v.fuelCost08 as fuel_cost,
    s.OVERALL_STARS as safety_overall_stars,
    s.FRNT_DRIV_STARS as front_stars,
    s.SIDE_DRIV_STARS as side_stars,
    s.ROLLOVER_STARS as rollover_stars
FROM dev.silver.fueleconomy_vehicles_silver v
LEFT JOIN dev.silver.fueleconomy_emissions_silver e
    ON v.id = e.id
LEFT JOIN dev.silver.nhtsa_safercar_silver s
    ON v.make = s.MAKE
   AND v.model = s.MODEL
   AND v.year = s.MODEL_YR