library(tidyverse)
source("02_scripts/ipf_balancing.R")

DATA_DIR <-file.path("01_data") 
LKUP_DIR <- file.path(DATA_DIR, "03_lkup")

# Load Data
gccsa_lkup <- read_csv(file.path(LKUP_DIR, "sa4_lkup.csv")) |> 
  distinct(gccsa_code, state_abrev)

pop_df <- arrow::read_parquet(file.path(DATA_DIR, "01_clean", "tmp_nim_df_2022.parquet"), 
                              col_select = c("id", "year", "age", "gender", "curr_gccsa_code", "lf_status", "flag_death", "flag_dep"))

flow_df <- arrow::read_parquet(file.path(DATA_DIR, "01_clean", "tm_lf_status_flow.parquet"))

# Prep ----
pop_df <- pop_df |> 
  filter(flag_death == 0, flag_dep == 0) |>
  rename(gccsa_code = curr_gccsa_code) |>
  mutate(
    age_10yr = case_when(
      age >= 15 & age <= 24 ~ "15-24 years",
      age >= 25 & age <= 34 ~ "25-34 years",
      age >= 35 & age <= 44 ~ "35-44 years",
      age >= 45 & age <= 54 ~ "45-54 years",
      age >= 55 & age <= 64 ~ "55-64 years",
      age >= 65  ~ "65 years and over"
    )
  ) |> 
  left_join(gccsa_lkup, join_by(gccsa_code)) |>
  mutate(
    grp_id = paste0(year,state_abrev, gender, age_10yr),
    grp_id2 = paste0(grp_id, "_", lf_status)
         )

state_pop <- pop_df |> 
  group_by(grp_id, lf_status) |> 
  summarise(pop = n(), .groups = 'drop')

state_flow <- flow_df |>
  filter(year == 2022) |>
  mutate(grp_id = paste0(year,state_abrev, gender, age_10yr)) |> 
  select(grp_id, lf_status, flow, value_flow)

####
#id = "2022ACTfemale15-24 years"
#id = "2022ACTmale15-24 years"

pop_flow_df <- state_flow |> 
  pivot_wider(names_from = flow, values_from = value_flow) |>
  left_join(
    state_pop,join_by(grp_id, lf_status)
    ) 

# Adjust the totals using IPF balancing 
lst = pop_flow_df |>  
  #filter(grp_id == id) |> 
  group_by(grp_id) |> 
  group_split() |> 
  map(~ {
    # pop
    tlt_pop <- .x |> filter(grp_id == .x$grp_id[1]) |> pull(pop)
    #tlt_pop
    tlt_col <- .x |> filter(grp_id == .x$grp_id[1]) |>
      select(-pop) |>
      summarise(across(where(is.numeric), sum)) |> unlist()
    #
    dat <- .x |> select(lf_status, employed, unemployed, nilf )
    ipf_balancing(dat, tlt_pop, tlt_col) |> mutate(grp_id = .x$grp_id[1])
    
  }) 

#lst |> bind_rows()

### or with loop
# ids = unique(pop_flow_df$grp_id)
# lst = list()
# for(i in 1: length(ids)){
#   grp_tmp <- pop_flow_df |> 
#     filter(grp_id == ids[i])
#   # targets to 
#   trg =  rowSums(grp_tmp[, 3:5])
#   pop = grp_tmp |> pull(pop)
#   
#   lst[[i]] = ipf_balancing(grp_tmp[,c(-1, -6)], pop , trg) |> 
#     mutate(grp_id = ids[i])
# 
# }

# pop_df <- pop_df %>%
#   mutate(grp_id2 = paste0(grp_id, "_", lf_status))

df_adj <- lst |> bind_rows() |>
            left_join(state_pop, join_by(lf_status, grp_id)) |>
            mutate(
              across(where(is.numeric), round),
              tlt = employed + unemployed + nilf,
              chk = ifelse(pop != tlt, 1, 0),
              nilf = ifelse(chk == 1, pop -(employed + unemployed), nilf),
              grp_id2 = paste0(grp_id, "_", lf_status)
            ) |>
            relocate(grp_id2) |>
            select(-pop, -tlt, -chk) |>
            pivot_longer(!c(grp_id,grp_id2, lf_status), names_to = "dest", values_to = "n")

df_adj 

# Randomly split
out_df <- pop_df |>   
group_by(grp_id2) %>%
  group_split() %>%
  map_df(~ {
    group_dests <- df_adj |> 
    filter(grp_id2 == .x$grp_id2[1]) 
    dest_vec <- rep(group_dests$dest, group_dests$n)
    .x %>%
      mutate(dest = sample(dest_vec))
  })
 
out_df 


pop_df |> filter(grp_id2 == "2022ACTfemale25-34 years_unemployed")
