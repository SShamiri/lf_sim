library(tidyverse)

# INPUTS ----
# transition matrix
tm_df <- read_csv("01_data/02_tm/tm_nat.csv")

# initial population
starting_pop <- 24195701  
# year for simulation
years <- 2017:2023 

# base_sim
tm_df <- tm_df |> 
  filter(!is.na(growth_rate))

# growth rates for each year from 2017 to 2023
#growth_rates <- c(0.016310046, 0.015814995, 0.01513017, 0.012338956, 0.009779412, 0.009888628, 0.009997092)

# POPULATION SIMULATION ----

# The years to simulate
pop <- numeric(length(years))  # Empty vector to store results

# first year (2017)
pop[1] <- starting_pop

# Simulate the population for each subsequent year
for (i in 2:c(length(years)+1) ) {
#pop[i] <- pop[i - 1] * (1 + growth_rates[i - 1])
  pop[i] <- pop[i - 1] * (1 + tm_df$growth_rate[i])
}

# Combine the results into a data frame
pop_df <- data.frame(
  year = c(years[1]-1,years), # either add the starting year or
  pop = round(pop)  # remove starting year pop [-1]
)
#pop_df |> clipr::write_clip()

# national LF
nat_df <-pop_df |> 
  left_join(tm_df, join_by(year)) |> 
  mutate(pop_15yr_over = pop * over_15yr_rate ,
         lf = pop_15yr_over * part_rate,
         emp = lf * emp_rate,
         unemp = lf * unemp_rate,
         nilf = pop_15yr_over - lf
         ) %>%
  select(year, !contains("rate"))

#nat_df |> clipr::write_clip()



######### other
data.frame(
  year = years, # either add the starting year or
  # pop = round(pop) , # remove starting year pop [-1]
  gr = growth_rates
) |> 
  clipr::write_clip()
