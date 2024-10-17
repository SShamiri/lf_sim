
lf_df <- read_csv("01_data/01_clean/lf_df.csv")

# INPUT
# 2016
base_pop <- 24195701

pop_growth <- tibble(year = 2017:2023,
                     gr_rate = c( 
                                  #0.0158
                                  0.0163
                                 ,0.0158
                                 ,0.0151
                                 ,0.0123
                                 ,0.0098
                                 ,0.0099
                                 ,0.0100
                     ))
  
  
# 
for(i in 2017: 2023){
  pop <- 
}


# Define initial population and growth rates
initial_population <- 24195701
growth_rates <- c(0.0163, 0.0158, 0.0151, 0.0123, 0.0098, 0.0099, 0.0100)
years <- 2017:2023

# Calculate population for each year
populations <- c(initial_population)
for (rate in growth_rates) {
  populations <- c(populations, tail(populations, 1) * (1 + rate))
  print(populations)
}

# Create a data frame with the results
df <- data.frame(
  Year = years,
  Population = populations,
  GrowthRate = c(0, growth_rates)  # Add 0 for the initial year
)
