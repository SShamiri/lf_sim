library(tidyverse)
library(janitor)

DATA_DIR <-file.path("01_data") 
LKUP_DIR <- file.path(DATA_DIR, "03_lkup")

# Load Data
pop_df <- arrow::read_parquet(file.path(DATA_DIR, "01_clean", "tmp_nim_df_2022.parquet"), 
                              col_select = c("id", "year", "age", "gender", "curr_gccsa_code", "prev_gccsa_code", "flag_death", "flag_dep"))

pop_df <- pop_df |> 
  rename(gccsa_code = curr_gccsa_code) |>    
  mutate(grp_id = paste0(year, gccsa_code, gender, age)) 

dat <- read_csv(file.path(DATA_DIR, "00_raw", "SummarisedCoPFlowImplications.csv")) |> 
  clean_names()

lkup <- tibble(
  gccsa_code = pop_df |> distinct(prev_gccsa_code) |> pull() |> dput(),
  gccsa = c("SYD", "RNSW", "MELB", "RVIC", "BRIS","RQLD", "ADEL", "RSA", "PER","RWA", "HOB", "RTAS","DAR",  
            "RNT", "ACT")
)


dep_df <- dat |> 
  filter(year == 2022, age >= 15) |> 
  select(year, gccsa, sex, age, syd:last_col()) |> 
  pivot_longer(!c(year:age), names_to = "dest_gccsa", values_to = "dep") |> 
  left_join(lkup, join_by(gccsa)) |>
  mutate(
    dest_gccsa = toupper(dest_gccsa),
    gender = ifelse(sex == "M", "male", "female")
  ) |>
  select(!c(sex, gccsa)) |>
  rename(gccsa = dest_gccsa) |>
  left_join(lkup |> 
              rename(dest_gccsa = gccsa_code), join_by(gccsa)
  ) |>
  select(!c(gccsa)) |>
  mutate(grp_id = paste0(year, gccsa_code, gender, age)) |>
  group_by(grp_id) |>
  mutate(tlt = sum(dep)) |>
  ungroup() 
  
# update dep to include grp pop
 dep_df <- pop_df |>
          select(grp_id) |>
          group_by(grp_id) |>
          summarise(pop = n(), .groups = 'drop') |>
          left_join(
            dep_df, join_by(grp_id)
          ) |>
          mutate(dep = ifelse(gccsa_code == dest_gccsa, pop -tlt, dep)) |>
          select(-tlt)

# random split
out_df <- pop_df |>   
  group_by(grp_id) %>%
  group_split() %>%
  map_df(~ {
    group_dests <- dep_df |> 
      filter(grp_id == .x$grp_id[1]) 
    dest_vec <- rep(group_dests$dest_gccsa, group_dests$dep)
    .x %>%
      mutate(curr_gccsa_code = sample(dest_vec))
  })
