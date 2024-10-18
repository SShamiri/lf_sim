library(tidyverse)

lf_df <- read_csv("01_data/01_clean/lf_df.csv")
pop_rate <- read_csv("01_data/01_clean/pop_rate.csv")


# year rates
lf_rate <-lf_df |>
  mutate(year = year(date)) |>
  group_by(year, type) |>
  summarise(value = mean(value), .groups = 'drop') |>
  filter(year >= 2015)  |>
  pivot_wider(names_from = type, values_from = value) |>
  mutate(part_rate = lf/pop_15yr_over,
         emp_rate = emp/lf,
         unemp_rate = unemp/lf
  ) %>%
  select(year, contains("rate"))

tm_df <- lf_rate |> 
  left_join(pop_rate, join_by(year))

# save
tm_df |> write_csv("01_data/02_tm/tm_nat.csv")
