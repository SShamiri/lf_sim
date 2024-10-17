library(tidyverse)
library(clipr)

source("00_util/read_abs.r")

# remove gender
lf_raw <- clean_abs_raw("01_data/00_raw/6202001.xlsx") |> 
  filter(grepl("Persons", data_item_description))


lf_df <- lf_raw %>%
  filter(data_type == "STOCK", series_type == "Original") |> 
  select(date, data_item_description, value) |> 
  mutate(type = case_when(
    data_item_description == "Employed total ;  Persons ;" ~ "emp",
    data_item_description == "Unemployed total ;  Persons ;" ~ "uemp",
    data_item_description == "Labour force total ;  Persons ;" ~ "lf",
    data_item_description == "Not in the labour force (NILF) ;  Persons ;" ~ "nilf",
    data_item_description == "Civilian population aged 15 years and over ;  Persons ;" ~ "pop_15yr_over"
  )) %>%
  filter(!is.na(type)) %>%
  select(-data_item_description) %>%
  relocate(type, .after = date) |> 
  mutate(value = value *1000)
  #pivot_wider(names_from = type, values_from = value) 

## below can be reproduced from values (above)
# lf_raw %>%
#   filter(data_type == "PERCENT", series_type == "Original") |>
#   select(date, data_item_description, value) |>
#   mutate(type = case_when(
#     data_item_description == "Participation rate ;  Persons ;" ~ "part_rate",
#     data_item_description == "Employment to population ratio ;  Persons ;" ~ "emp_pop_rate",
#     data_item_description == "Unemployment rate ;  Persons ;" ~ "unemp_rate"
#   )) %>%
#   filter(!is.na(type)) %>%
#   select(-data_item_description) %>%
#   relocate(type, .after = date) |>
#   pivot_wider(names_from = type, values_from = value) |>
#   write_clip()
#  
# lf_df |> 
#   mutate(year = year(date)) |> 
#   group_by(year, type) |> 
#   summarise(value = mean(value), .groups = 'drop') |> 
#   filter(year >= 2015)  |> 
#   pivot_wider(names_from = type, values_from = value) |> 
#   write_clip()

# save

lf_df |> 
  write_csv("01_data/01_clean/lf_df.csv")
