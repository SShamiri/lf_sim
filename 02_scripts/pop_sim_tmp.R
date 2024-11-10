library(tidyverse)
library(janitor)
library(tictoc)

dat <- read_csv("01_data/00_raw/SummarisedCoPFlowImplications.csv") |> 
  clean_names()

pop_df <- dat |> 
  filter(year == 2022) |> 
  #filter(year == 2022, age %in% c(28, 30), pop > 0, gccsa %in% c("SYD", "MELB")) |> 
  select(year, gccsa, sex, age, pop) |> 
  uncount(pop) |>
  mutate(id = row_number()) |> 
  relocate(id) |> 
  group_by(year, gccsa, sex, age) |> 
  mutate(group_id = cur_group_id()) |> 
  ungroup() |> 
  arrange(group_id)

pop_df 

dep_df <- dat |> 
  filter(year == 2022) |> 
  #filter(year == 2022, age %in% c(28, 30), pop > 0, gccsa %in% c("SYD", "MELB")) |> 
  select(year, gccsa, sex, age, 14:last_col()) |> 
  pivot_longer(!c(year:age), names_to = "dest_gccsa", values_to = "dep") |> 
  group_by(year, gccsa, sex, age) |> 
  mutate(group_id = cur_group_id()) |> 
  ungroup() |> 
  arrange(group_id)

dep_df

id_gr <- unique(pop_df$group_id)
id_dep <- dep_df |> distinct(gccsa,dest_gccsa, dep)

lst = list()
ll <- list()

tic()
for(i in 1: length(id_gr)){
  #i = 2
  tmp_dat <- pop_df |> 
    filter(group_id == i) 
  dd = dep_df|> 
    filter(group_id == i) 

    for(j in 1:nrow(dd)) {
      #j = 2
      lst[[j]] <- tmp_dat |> 
        sample_n(size = dd$dep[j]) |> 
        mutate(gg = dd$dest_gccsa[j])
     
      tmp_dat <- tmp_dat |> filter(!id %in% lst[[j]]$id)
      #print(lst[[j]])
    }
  dt = lst |> bind_rows()
  ll[[i]] <- dt
}  
toc()

ll

out <- ll |> bind_rows() 

out
out |> group_by(year, gccsa, sex, age ,gg) |>
  summarise(n = n(), .groups = 'drop') |>
  pivot_wider(names_from = gg, values_from = n)

out |> 
  group_by(id) |> 
  summarise(n = n()) |> 
  filter(n > 1)
