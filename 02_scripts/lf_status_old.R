library(tidyverse)


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
  mutate(grp_id = paste0(year,state_abrev, gender, age_10yr))

state_pop <- pop_df |> 
  group_by(grp_id, lf_status) |> 
  summarise(pop = n(), .groups = 'drop')

state_flow <- flow_df |>
  filter(year == 2022) |>
  mutate(grp_id = paste0(year,state_abrev, gender, age_10yr)) |> 
  select(grp_id, lf_status, flow, value_flow)

# check if 2022 pop in pop and flow are the same
sum(state_pop$pop) # 21,245,034
sum(flow_df[flow_df$year == 2022,]$value_flow) # 21,245,013

# example ----
grp_id = "2022QLDfemale15-24 years"

# targets
trg_value <- state_flow |> 
  filter(grp_id == !!grp_id, lf_status == flow ) |> 
  pull(value_flow)

trg_name <- state_flow |> 
  filter(grp_id == !!grp_id, lf_status == flow ) |> 
  pull(flow)

names(trg_value) <- trg_name

trg_df <- state_pop |> 
  filter(grp_id == !!grp_id) |> 
  left_join(
    state_flow |> 
      filter(grp_id == !!grp_id ),
    join_by(grp_id, lf_status)
  ) |> 
  group_by(grp_id, flow) |>
  mutate(
    prop = value_flow/sum(value_flow),
    smpl = case_when(
      flow == "employed" ~ prop * trg_value['employed'],
      flow == "unemployed" ~ prop * trg_value['unemployed'],
      flow == "nilf" ~ prop * trg_value['nilf'],
    ),
    smpl = round(smpl,0)
  ) |>
  ungroup() |> 
  arrange(flow)

# Flow to employed
flow_to_name <- "employed"
emp_smpl <- trg_df |> 
  filter(flow == !!flow_to_name) |>
  mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) |> # this needs to be check  
  # Perhaps to reach the target take more from nilf
  pull(smpl)

emp_lst = list()
for(i in 1:length(emp_smpl)){
  emp_lst[[i]] <- pop_df |> 
                  filter(grp_id == !!grp_id, lf_status == trg_name[i]) |> 
                  sample_n(size = emp_smpl[i]) |>
                  mutate(flow_to = !!flow_to_name)
}
emp_df <- bind_rows(emp_lst)

# Flow to unemployed
flow_to_name <- "unemployed"
unemp_smpl <- trg_df |> 
  filter(flow == !!flow_to_name) |>
  mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) |> # this needs to be check  
  # Perhaps to reach the target take more from nilf
  pull(smpl)

unemp_lst = list()
for(i in 1:length(unemp_smpl)){
  unemp_lst[[i]] <- pop_df |> 
            filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% emp_df$id) |> 
            sample_n(size = unemp_smpl[i]) |>
            mutate(flow_to = !!flow_to_name) 
  }
unemp_df <- bind_rows(unemp_lst)

# put grp together emp, unemp and nilf
grp_df_tmp <- bind_rows(emp_df, unemp_df)
grp_df <- pop_df |> 
            filter(grp_id == !!grp_id) |>
            left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
            mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))
  
grp_df

### end
pop_df |>
  filter(grp_id == !!grp_id) |>
  left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
  mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to)) |>
  group_by(grp_id, flow_to) |>
  summarise(trgt = n(), .groups = 'drop') |>
  pivot_wider(names_from = flow_to, values_from = trgt) |>
  mutate(
    lf = employed + unemployed,
    unemp_rate = unemployed/lf
  )


trg_df |> 
  filter(flow == !!flow_to_name) |>
  mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl))

pop_df |> 
  filter(grp_id == !!grp_id, lf_status == trg_name[1], !id %in% emp_df$id)
