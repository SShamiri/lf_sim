# Databricks notebook source
# MAGIC %md
# MAGIC ## Set directories

# COMMAND ----------

# Directories
TEAM_DIR <- file.path("", "dbfs", "mnt", "business-purpose", "skills_supply_demand_model")
# # ml-dev environment
# TEAM_DIR <- file.path("/Volumes/jsa_external_prod/external_vols", "business_purpose", "skills_supply_demand_model")

PROJ_DIR <- file.path(TEAM_DIR,  "01_projects", "00_skills_supply_model")
DATA_DIR <- file.path(TEAM_DIR, "00_data")
IN_DIR <- file.path(PROJ_DIR, "00_dev","01_gold")
LKUP_DIR <- file.path(DATA_DIR, "02_lookup")
PKG_DIR <- file.path(TEAM_DIR, "02_r_pkg")
FEATURE_DIR <- file.path(PROJ_DIR, "00_dev","00_features")
TEST_DIR <- file.path(PROJ_DIR,"00_dev","02_test")

# COMMAND ----------

# MAGIC %run ../utils/pkg_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Packages

# COMMAND ----------

# persist packages
persist_path(new_path = PKG_DIR)

# COMMAND ----------

packages <- c(
              "dplyr",
              "lubridate",
              "tidyr",
              "readr",
              "purrr",
              "tictoc",
              "collapse"
             )

# install packages not previously installed
pkg_install(packages)

# package check, persist only new packages
pkg_checkpoint()

# COMMAND ----------

# MAGIC %run ./micro_sim_fun

# COMMAND ----------

# MAGIC %md ## Load data

# COMMAND ----------

# MAGIC %md ## lf flow

# COMMAND ----------

#graduate to employment
grademp_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_study_to_emp_loefoe_lfs_occ4.parquet"))

# COMMAND ----------

nim_df_tmp <- arrow::read_parquet(file.path(IN_DIR, "tmp_nim_df_2022.parquet"))

# COMMAND ----------

# lf
#lf_flow_df <- arrow::read_parquet(file.path(IN_DIR, "tm_lf_status_flow.parquet"))
lf_flow_df <- arrow::read_parquet(file.path(IN_DIR, "tm_lf_status_flow_disag.parquet"))

# COMMAND ----------

pop_df <- nim_df_tmp |>
          fsubset(flag_death == 0 & flag_dep == 0) |> 
          select(id, year, gender, age, gccsa_code = curr_gccsa_code, lf_status) |>
          mutate(grp_id = paste0(year, gccsa_code, gender, age))

# pop_df <- grademp_df_tmp  |>
#           fsubset(flag_death == 0 & flag_dep == 0) |> 
#           select(id, year, gender, age, gccsa_code = curr_gccsa_code, lf_status) |>
#           mutate(grp_id = paste0(year, gccsa_code, gender, age))

# COMMAND ----------

sim_year = 2022:2034
sim_year

# COMMAND ----------

i = 1
tictoc::tic()
lf_status_df <- lf_status_fun(nim_df_tmp)
tm <- tictoc::toc(log = TRUE, quiet = TRUE)
tm_msg(tm$callback_msg)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ##Steps

# COMMAND ----------

grp_pop = pop_df |>
          group_by(grp_id, lf_status) |> 
          summarise(pop = n(), .groups='drop') 

# COMMAND ----------

sim_year <- 2022
lf_flow <- lf_flow_df |>
            filter(year ==!!sim_year[1]) |>
            mutate(grp_id = paste0(year,gccsa_code, gender, age)) |> 
            #select(grp_id, lf_status, flow = lf_flow, value_flow = lf_target)
            select(grp_id, lf_status, flow_to, lf_target, smpl)

# COMMAND ----------

#lf_flow_df |> display()

# COMMAND ----------

# grp_flow <- lf_flow |> 
#     left_join(
#     grp_pop, join_by(grp_id, lf_status)
#     ) 

grp_flow <- grp_pop |> 
    left_join(
    lf_flow, join_by(grp_id, lf_status)
    ) 

# COMMAND ----------

## First Flow to emplyement
# correct the target (sample) size with the population 
emp_flow <- grp_flow |>
        group_split(grp_id) |>
        map_dfr(~ emp_by_group(.x, .x$grp_id[1]))

# COMMAND ----------

# # Expand the counts into individual assignments
expanded_counts_emp <- emp_flow |> uncount(smpl)

# COMMAND ----------

emp_df <- pop_df |> 
        group_by(grp_id) %>%
        group_split() %>%
          map_df(~ {
            print(.x$grp_id[1])
            # Get population and assignment needs for this group
            pop_ids <- pop_df %>% filter(grp_id == .x$grp_id[1]) %>% pull(id) %>% sample()
            assigns <- expanded_counts_emp %>% filter(grp_id == .x$grp_id[1])         
            # Assign IDs to destinations
            tibble(
              id = pop_ids[seq_len(nrow(assigns))],
              grp_id = .x$grp_id[1],
              flow_to = assigns$flow_to
            )
          })# 28 min ## GOOD

# COMMAND ----------

##### second Flow to unemplyement
# update grp_flow to exlucde emp ids
 pop_size <- pop_df |> 
      filter(!id %in% emp_df$id) |>
      group_by(grp_id, lf_status) |>
      summarise(pop = n(), .groups= 'drop')

 grp_flow_unemp <-  grp_flow |> 
           filter(flow_to == "unemployed") |>
            select(-pop) |>
            left_join(pop_size, join_by(grp_id, lf_status))


# COMMAND ----------

unemp_flow <-  grp_flow_unemp |>
        group_split(grp_id) |>
        map_dfr(~ unemp_by_group(.x, .x$grp_id[1]))

# COMMAND ----------

# # Expand the counts into individual assignments
expanded_counts_unemp <- unemp_flow |> uncount(smpl)

# COMMAND ----------

unemp_df = pop_df |> 
        filter(!id %in% emp_df$id) |>
        group_by(grp_id) %>%
        group_split() %>%
          map_df(~ {
            print(.x$grp_id[1])
            # Get population and assignment needs for this group
            pop_ids <- .x %>% filter(grp_id == .x$grp_id[1]) %>% pull(id) %>% sample()
            assigns <- expanded_counts_unemp %>% filter(grp_id == .x$grp_id[1])         
            # Assign IDs to destinations
            tibble(
              id = pop_ids[seq_len(nrow(assigns))],
              grp_id = .x$grp_id[1],
              flow_to = assigns$flow_to
            )
          })# 45 sec ## GOOD

# COMMAND ----------

# put grp together emp, unemp and nilf
grp_df_tmp <- bind_rows(emp_df, unemp_df)

out_df <- pop_df |> 
    left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
    mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))

# COMMAND ----------

out_df|>
#filter(grp_id == !!id[1]) |>
group_by(flow_to) |>
summarise(n = n(), .groups='drop') |>
pivot_wider(names_from= flow_to, values_from=n) |>
mutate(
  lf = employed + unemployed,
  unemp_rate = unemployed /lf
) |>
display()

# COMMAND ----------

# compare above with
lf_flow |>
#filter(grp_id == !!id[1]) |>
group_by(flow_to) |>
summarise(n = sum(smpl), .groups='drop') |>
pivot_wider(names_from= flow_to, values_from=n) |>
mutate(
  lf = employed + unemployed,
  unemp_rate = unemployed /lf
) |>
display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## New func

# COMMAND ----------

emp_f <- grp_flow |>
        group_split(grp_id) |>
        map_dfr(~ emp_by_group(.x$grp_id[1]))

# COMMAND ----------

# # Expand the counts into individual assignments
expanded_counts <- emp_f %>%
  uncount(smpl)

# COMMAND ----------

# Apply the assignment for each group
#result <- map_dfr(unique(emp_f$grp_id), assign_by_group)
$result <- map_dfr(unique(tt$grp_id), assign_by_group)

# COMMAND ----------

# Function to assign individuals by group
emp_by_group <- function(grp_id) {
  ## Flow to employed
  emp_smpl <- grp_flow |> 
    filter(grp_id == !!grp_id, flow_to == "employed") |>
    mutate(
      pop = ifelse(is.na(pop), 0, pop),
      chk = ifelse(smpl > pop, 1, 0)) 
  # check if the sample is greater than pop  
  if(sum(emp_smpl$chk) > 0) {
      emp_smpl <- emp_smpl |>
                    mutate(
                      pop_tlt = sum(pop),
                      scale_factor = lf_target/pop_tlt,
                      smpl = scale_factor * pop,
                      smpl = GFE::round_preserve_sum(smpl, 0)
                    ) |>
                    select(!c(chk, pop_tlt, scale_factor))
                    }
  emp_smpl
}

# COMMAND ----------

dd = pop_df |> 
#filter(grp_id %in% c("20221GSYDfemale15"))  |>
        group_by(grp_id) %>%
        group_split() %>%
          map_df(~ {
            print(.x$grp_id[1])
            # Get population and assignment needs for this group
            pop_ids <- pop_df %>% filter(grp_id == .x$grp_id[1]) %>% pull(id) %>% sample()
            assigns <- expanded_counts %>% filter(grp_id == .x$grp_id[1])         
            # Assign IDs to destinations
            tibble(
              id = pop_ids[seq_len(nrow(assigns))],
              grp_id = .x$grp_id[1],
              flow_to = assigns$flow_to
            )
          })# 28 min ## GOOD

# COMMAND ----------

# update grp_flow to exlucde emp ids
 pop_size <- pop_df |> 
      filter(!id %in% dd$id) |>
      group_by(grp_id, lf_status) |>
      summarise(pop = n(), .groups= 'drop')

# COMMAND ----------

 grp_flow_un <-  grp_flow |> 
           filter(flow_to == "unemployed") |>
            select(-pop) |>
            left_join(pop_size, join_by(grp_id, lf_status))

# COMMAND ----------

unemp_f <-  grp_flow_un|>
        group_split(grp_id) |>
        map_dfr(~ unemp_by_group(.x$grp_id[1]))

# COMMAND ----------

# # Expand the counts into individual assignments
expanded_counts_unemp <- unemp_f %>%
  uncount(smpl)

# COMMAND ----------

dd_uemp = pop_df |> 
filter(!id %in% dd$id) |>
        group_by(grp_id) %>%
        group_split() %>%
          map_df(~ {
            print(.x$grp_id[1])
            # Get population and assignment needs for this group
            pop_ids <- .x %>% filter(grp_id == .x$grp_id[1]) %>% pull(id) %>% sample()
            assigns <- expanded_counts_unemp %>% filter(grp_id == .x$grp_id[1])         
            # Assign IDs to destinations
            tibble(
              id = pop_ids[seq_len(nrow(assigns))],
              grp_id = .x$grp_id[1],
              flow_to = assigns$flow_to
            )
          })# 45 sec ## GOOD

# COMMAND ----------

# put grp together emp, unemp and nilf
grp_df_tmp <- bind_rows(dd, dd_uemp)


# COMMAND ----------

out_df <- pop_df |> 
    left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
    mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))

# COMMAND ----------

out_df |>
group_by(year, flow_to) |>
summarise(n = n(), .groups='drop') |>
pivot_wider(names_from=flow_to, values_from=n) |>
mutate(
  lf = employed + unemployed,
  u_rate = unemployed/lf
)

# COMMAND ----------

unemp_by_group <- function(grp_id) {
unemp_smpl <-  grp_flow_un |> 
           filter(grp_id == !!grp_id, flow_to == "unemployed") |>
            #select(-pop) |>
            #left_join(pop_size, join_by(grp_id, lf_status)) |>
            mutate(
              pop = ifelse(is.na(pop), 0, pop),
              chk = ifelse(smpl > pop, 1, 0)
              ) 

if(sum(unemp_smpl$chk) > 0) {
 unemp_smpl <- unemp_smpl |>
              mutate(
                pop_tlt = sum(pop),
                scale_factor = lf_target/pop_tlt,
                smpl = scale_factor * pop,
                smpl = GFE::round_preserve_sum(smpl, 0)
              ) |>
              select(!c(chk, pop_tlt, scale_factor))
              }
  unemp_smpl
}

# COMMAND ----------

# # Function to assign individuals by group
# assign_by_group <- function(grp_id) {
#   # Get population and assignment needs for this group
#   pop_ids <- pop_df %>% filter(grp_id == grp_id) %>% pull(id) %>% sample()
#   assigns <- expanded_counts %>% filter(grp_id == grp_id)
  
#   # Assign IDs to destinations
#   tibble(
#     id = pop_ids[seq_len(nrow(assigns))],
#     grp_id = grp_id,
#     flow_to = assigns$flow_to
#   )
# }

# COMMAND ----------

emp_smpl

# COMMAND ----------

# Expand the counts into individual assignments
expanded_counts <- emp_smpl %>%
  uncount(smpl)

# COMMAND ----------

expanded_counts

# COMMAND ----------

# Function to assign individuals by group
assign_by_group <- function(group) {
  # Get population and assignment needs for this group
  pop_ids <- pop_df %>% filter(grp_id == group) %>% pull(id) %>% sample()
  assigns <- expanded_counts %>% filter(grp_id == group)
  
  # Assign IDs to destinations
  tibble(
    id = pop_ids[seq_len(nrow(assigns))],
    grp_id = group,
    flow_to = assigns$flow_to
  )
}

# COMMAND ----------

# Apply the assignment for each group
result <- map_dfr(unique(emp_smpl$grp_id), assign_by_group)

# COMMAND ----------

result 

# COMMAND ----------

# Function to assign individuals by group
assign_by_group <- function(group) {
  # Get population and assignment needs for this group
  pop_ids <- vpop %>% filter(grp == group) %>% pull(id) %>% sample()
  assigns <- expanded_counts %>% filter(grp == group)
  
  # Assign IDs to destinations
  tibble(
    id = pop_ids[seq_len(nrow(assigns))],
    grp = group,
    dest = assigns$dest
  )
}

# Apply the assignment for each group
result <- map_dfr(unique(counts$grp), assign_by_group)


# COMMAND ----------

# Input data
vpop <- data.frame(
  id = c(paste0("A-", 1:9), paste0("B-", 1:7)),
  grp = c(rep("A", 9), rep("B", 7))
)

counts <- data.frame(
  grp = c("A", "A", "A", "B", "B", "B"),
  dest = c("emp", "unemp", "nilf", "emp", "unemp", "nilf"),
  n = c(3, 3, 2, 3, 2, 1)
)

# Expand the counts into individual assignments
expanded_counts <- counts %>%
  uncount(n)

# COMMAND ----------



# COMMAND ----------

###########################################################################

# COMMAND ----------

# targets
  grp_id = id
  
  trg_value <- lf_flow |> 
    filter(grp_id == !!grp_id, lf_status == flow ) |> 
    pull(value_flow)
  
  trg_name <- lf_flow |> 
    filter(grp_id == !!grp_id, lf_status == flow ) |> 
    pull(flow)
  
  names(trg_value) <- trg_name
  
  trg_df <- grp_pop |> 
    filter(grp_id == !!grp_id) |> 
    left_join(
      lf_flow |> 
        filter(grp_id == !!grp_id ),
      join_by(grp_id, lf_status)
    ) |> 
    group_by(grp_id, flow) |>
    mutate(
     # prop = value_flow/sum(value_flow),
      prop = ifelse(is.nan(value_flow/sum(value_flow)),0, value_flow/sum(value_flow)),
      smpl = case_when(
        flow == "employed" ~ prop * trg_value['employed'],
        flow == "unemployed" ~ prop * trg_value['unemployed'],
        flow == "nilf" ~ prop * trg_value['nilf'],
      ),
      smpl = round(smpl,0)
    ) |>
    ungroup() |> 
    arrange(flow)

# COMMAND ----------

# Flow to employed
  flow_to_name <- "employed"
  emp_smpl <- trg_df |> 
    filter(flow == !!flow_to_name) |>
    mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) #|> # this needs to be check       
   # pull(smpl)
  
  emp_lst = list()
  # for(i in 1:length(emp_smpl)){
  #   emp_lst[[i]] <- pop_df |> 
  #     filter(grp_id == !!grp_id, lf_status == trg_name[i]) |> 
  #     sample_n(size = emp_smpl[i]) |>
  #     mutate(flow_to = !!flow_to_name)
  # }

  for(i in 1:nrow(emp_smpl)){
    emp_lst[[i]] <- pop_df |> 
       filter(grp_id == !!grp_id, lf_status == emp_smpl$lf_status[i]) |>
       sample_n(size = emp_smpl$smpl[i]) |>
      mutate(flow_to = !!flow_to_name)
  }
  emp_df <- bind_rows(emp_lst)

# COMMAND ----------

emp_df

# COMMAND ----------

pop_df |> 
      filter(grp_id == !!grp_id, lf_status == emp_smpl$lf_status[1]) |>
       sample_n(size = emp_smpl$smpl[1]) |>
      mutate(flow_to = !!flow_to_name)

# COMMAND ----------

trg_name[2]

# COMMAND ----------

pop_df |> 
      filter(grp_id == !!grp_id) # only nilf

# COMMAND ----------

# Flow to unemployed
flow_to_name <- "unemployed"
# sample size
  unemp_smpl <- trg_df |> 
    filter(flow == !!flow_to_name) |>
    mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) |>
    select(grp_id, lf_status, smpl)

# pop size
 pop_size = pop_df |> 
      #filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% dat$id) 
      filter(grp_id == !!grp_id, !id %in% emp_df$id) |>
      group_by(grp_id, lf_status) |>
      summarise(pop = n(), .groups= 'drop')

# Check point 
chk_df = pop_size |>
        left_join(unemp_smpl, join_by(grp_id, lf_status)) |>
        mutate(chk = ifelse(smpl > pop, 0, 1))

if(any(chk_df$chk ==0)) {
  unemp_smpl <- smpl_check(chk_df, target= round(trg_value[names(trg_value) == flow_to_name],0)) |> pull(smpl)
} else {
  unemp_smpl <- unemp_smpl |> pull(smpl)
}



# COMMAND ----------

unemp_smpl

# COMMAND ----------

  unemp_lst = list()
  for(i in 1:length(unemp_smpl)){
    tmp <- pop_df |> 
      filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% emp_df$id)

    if(nrow(tmp) >=  unemp_smpl[i]) {
    unemp_lst[[i]] <- tmp |> sample_n(size = unemp_smpl[i]) |>
                      mutate(flow_to = !!flow_to_name)
} else {
    unemp_lst[[i]]<- tmp |> sample_n(size = 0) |>
                     mutate(flow_to = !!flow_to_name)
}
  }
  unemp_df <- bind_rows(unemp_lst)

# COMMAND ----------

 unemp_df

# COMMAND ----------

 i = 2 # step by
dd = pop_df |> 
      filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% emp_df$id) 
if(nrow(dd) >=  unemp_smpl[i]) {
  out <- dd |> sample_n(size = unemp_smpl[i]) |>
      mutate(flow_to = !!flow_to_name)
} else {
    out <- dd |> sample_n(size = 0) |>
      mutate(flow_to = !!flow_to_name)
}

# COMMAND ----------

out

# COMMAND ----------

 i = 3 # step by
 pop_df |> 
      filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% emp_df$id) |> 
      sample_n(size = unemp_smpl[i]) |>
      mutate(flow_to = !!flow_to_name)

# COMMAND ----------

i = 3 # step by
  pop_df |> 
      filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% dat$id) |> # pop is 377 for nilf i = 2
      sample_n(size = unemp_smpl[i]) |> # smpl 715 for nilf i = 2
      mutate(flow_to = !!flow_to_name) |>
      select(id, grp_id, lf_status, flow_to)

# COMMAND ----------

# smpl size
unemp_smpl <- trg_df |> 
    filter(flow == !!flow_to_name) |>
    mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) |>
    select(grp_id, lf_status, smpl)

unemp_smpl 

# COMMAND ----------

 # pop size
 pop_size = pop_df |> 
      #filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% dat$id) 
      filter(grp_id == !!grp_id, !id %in% dat$id) |>
      group_by(grp_id, lf_status) |>
      summarise(pop = n(), .groups= 'drop')
pop_size

# COMMAND ----------

dd = pop_size |>
left_join(unemp_smpl) |>
mutate(chk = ifelse(pop > smpl, 1, 0)) 
# |>
# display()
# create a function that smpl is always smaller or equal to pop, however the total of the smpl is equal to the target (5982)

# COMMAND ----------

smpl_check(dd, target=5982)

# COMMAND ----------



# COMMAND ----------

smpl_check <- function(dat, target = 5982){
  # Adjust the values for smpl to be <= pop
  dat$smpl <- pmin(dat$smpl, dat$pop)
  # Calculate the difference to reach the total of 5982
  total_smpl <- sum(dat$smpl)
  difference <- target- total_smpl
  # Distribute the difference proportionally to the rows where smpl < pop
  while (difference != 0) {
    for (i in 1:nrow(dat)) {
      if (difference == 0) break
      if (dat$smpl[i] < dat$pop[i]) {
        dat$smpl[i] <- dat$smpl[i] + 1
        difference <- difference - 1
      }
    }
  }
  return(dat)

}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Other

# COMMAND ----------

lf_flow_df_agg |>
group_by(year, lf_flow) |>
summarise(n = sum(lf_target), .groups='drop') |>
pivot_wider(names_from= lf_flow, values_from=n) |>
mutate(
  lf = employed + unemployed,
  unemp_rate = unemployed /lf
) |>
display()

# COMMAND ----------

lf_flow_df |>
group_by(year, lf_flow) |>
summarise(n = sum(lf_target), .groups='drop') |>
pivot_wider(names_from= lf_flow, values_from=n) |>
mutate(
  lf = employed + unemployed,
  unemp_rate = unemployed /lf
) |>
display()

# COMMAND ----------



# COMMAND ----------

df <- data.frame(
  status = c("A", "C", "B"),
  pop = c(19289, 377, 1029),
  smpl = c(4976, 715, 291)
)

# Adjust the values for smpl to be <= pop
df$smpl <- pmin(df$smpl, df$pop)

# Calculate the difference to reach the total of 5982
total_smpl <- sum(df$smpl)
difference <- 5982 - total_smpl

# Distribute the difference proportionally to the rows where smpl < pop
while (difference != 0) {
  for (i in 1:nrow(df)) {
    if (difference == 0) break
    if (df$smpl[i] < df$pop[i]) {
      df$smpl[i] <- df$smpl[i] + 1
      difference <- difference - 1
    }
  }
}


print(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## steps

# COMMAND ----------

lf_flow_df_agg |>
# group_by(year, lf_flow) |>
# summarise(n = sum(value_flow), .groups='drop') |>
# pivot_wider(names_from= lf_flow, values_from=n) |>
# mutate(
#   lf = employed + unemployed,
#   unemp_rate = unemployed /lf
# ) |>
display()

# COMMAND ----------

lf_flow_df

# COMMAND ----------

lf_flow_df |>
filter(is.na(value_flow)) |>
#distinct(age) |>
mutate(value_flow = ifelse(is.na(value_flow) & age >= 90 & lf_flow == "nilf", pop, 0)
) |>
display()

# COMMAND ----------

lf_flow_df |>
group_by(year, lf_flow) |>
summarise(n = sum(value_flow), .groups='drop') |>
pivot_wider(names_from= lf_flow, values_from=n) |>
mutate(
  lf = employed + unemployed,
  unemp_rate = unemployed /lf
) |>
display()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

