library(tidyverse)
library(janitor)
library(tictoc)

source("02_scripts/sim_functions.R")

dat <- read_csv("01_data/00_raw/SummarisedCoPFlowImplications.csv") |> 
  clean_names()

base_df <- arrow::read_parquet("01_data/01_clean/pop_2021.parquet")
# Pop
pop_df <- base_df |> 
  mutate(year = year + 1,
         age = age + 1,
         id = sprintf('base%06d',row_number())) |> 
  relocate(id)

pop_df

# Depart
dep_df <- dat |> 
  filter(year > 2021, age >= 15) |> 
  select(year, gccsa, sex, age, pop, syd:last_col()) |> 
  pivot_longer(!c(year: pop), names_to = 'dest', values_to = 'dep') |> 
  mutate(dest = toupper(dest),
         #dep = ifelse(gccsa == dest, pop, dep)
         )

dep_df

## Sample for testing
smp_pop <- pop_df |> filter(gccsa == "SYD", sex == "M", age == 38)
smp_dep <- dep_df |> filter(year == 2022, gccsa == "SYD", sex == "M", age == 38, dep > 0)
dest_dep <- departures_fn(smp_pop, smp_dep$dep, dest_name = smp_dep$dest)
dd <- smp_pop |> 
  left_join(dest_dep |> 
              select(id, dest), join_by(id)) |> 
  mutate(dest = ifelse(is.na(dest), gccsa, dest)) 
dd |> 
  group_by(year, dest, sex, age ) |> 
  summarise(n = n())

###

group_vars(tt)
gg = group_vars(tt)

tt = pop_df |> 
  left_join(dep_df, join_by(year, gccsa, sex, age))
  group_by(year, gccsa, sex, age) |> 
  mutate(gr_id = cur_group_id())

tic()  
ttt = tt |> 
  #filter(gccsa == "SYD", sex == "M", age ==38) |> 
  group_by(year, gccsa, sex, age) |> 
  #mutate(cc_gccsa = gccsa) |> 
  group_map(~departures_fn(.x, 
                           .x |> filter(dep > 0) |> distinct(dest, dep) |> pull(dep) ,
                           .x |> filter(dep > 0) |> distinct(dest) |> pull(dest))) 
toc()


pop_df |>
  filter(gccsa == "SYD", sex == "M", age == 38) |> 
  left_join(
    ttt |> bind_rows() |> 
      select(id, dest),
    join_by(id)
  ) |> 
  mutate(dest = ifelse(is.na(dest), gccsa, dest)) |> 
  group_by(dest) |> 
  summarise(n = n())
  



ttt |> 
  mutate(data2 = map(., ~select(., id, dest)))



tt |> 
  filter(gccsa == "SYD", sex == "M", age %in% c(38)) |> 
  group_by(year, gccsa, sex, age)  |> 
  filter(dep > 0) |> 
  distinct(dest, dep) |>
  pull(dep)



ttt |> 
  group_by(dest) |> 
  summarise(n = n())

dep_df |> 
  filter(gccsa == "SYD", sex == "M", age %in% c(28,38))
