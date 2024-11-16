library(tidyverse)
library(janitor)
library(tictoc)

dat <- read_csv("01_data/00_raw/SummarisedCoPFlowImplications.csv") |> 
  clean_names()

dat |> 
  filter(year == 2021, age >= 15, pop > 0) |> 
  select(year, gccsa, sex, age, pop) |> 
  uncount(pop) |> 
  arrow::write_parquet("01_data/01_clean/pop_2021.parquet")
