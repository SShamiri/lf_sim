library(tidyverse)

# INPUTS ----
lf_df <- read_csv("01_data/01_clean/lf_df.csv")
tm_df <- read_csv("01_data/01_clean/tm.csv")

# initial population
starting_pop <- 24195701  
# sim years
years <- 2017:2023 
# growth rates for each year from 2017 to 2023
growth_rates <- c(0.016310046, 0.015814995, 0.01513017, 0.012338956, 0.009779412, 0.009888628, 0.009997092 )

# POPULATION SIMULATION ----

# The years to simulate
pop <- numeric(length(years))  # Empty vector to store results

# first year (2017)
pop[1] <- starting_pop

# Simulate the population for each subsequent year
for (i in 2:c(length(years) + 1)) {
  pop[i] <- pop[i - 1] * (1 + growth_rates[i - 1])
}

# Combine the results into a data frame
pop_df <- data.frame(
  year = c(years[1]-1,years), # either add the starting year or
  pop = round(pop)  # remove starting year pop [-1]
)
#pop_df |> clipr::write_clip()

# LF
#pop_15yr_over_rate <- c( 0.81,0.81,0.81,0.81,0.81,0.81,0.83)
pop_15yr_over_rate <- tm_df$over_15yr_rate
  
pop_df %>%
  slice(-1) |> 
  mutate(over_15 = pop * pop_15yr_over_rate[-1])

######### other
data.frame(
  year = years, # either add the starting year or
  # pop = round(pop) , # remove starting year pop [-1]
  gr = growth_rates
) |> 
  clipr::write_clip()


