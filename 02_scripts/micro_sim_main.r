# Databricks notebook source
# MAGIC %md # Simulation process

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set directories

# COMMAND ----------

# Directories
TEAM_DIR <- file.path("", "dbfs", "mnt", "business-purpose", "skills_supply_demand_model")
# ml-dev environment
#TEAM_DIR <- file.path("/Volumes/jsa_external_prod/external_vols", "business_purpose", "skills_supply_demand_model")

PROJ_DIR <- file.path(TEAM_DIR,  "01_projects", "00_skills_supply_model")
DATA_DIR <- file.path(TEAM_DIR, "00_data")
IN_DIR <- file.path(PROJ_DIR, "00_dev","01_gold")
PKG_DIR <- file.path(TEAM_DIR, "02_r_pkg")
FEATURE_DIR <- file.path(PROJ_DIR, "00_dev","00_features")

TEST_DIR <- file.path(PROJ_DIR,"00_dev","02_test")

# COMMAND ----------

# MAGIC %run ./micro_sim_fun

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
              "purrr",
              "tictoc",
              "collapse"
             )
# install packages not previously installed
pkg_install(packages)

# package check, persist only new packages
pkg_checkpoint()

# COMMAND ----------

# MAGIC %md ## Load data

# COMMAND ----------

# data
 base_df <- arrow::read_parquet(file.path(IN_DIR, "spop_census_2021_visa_countdown.parquet"))

# COMMAND ----------

 #New entrants
new_entrants <-arrow::read_parquet(file.path(IN_DIR, "tm_new_entrants.parquet")) 
# NOM
arr_df <- arrow::read_parquet(file.path(IN_DIR, "tm_nom_arr.parquet")) 
nom_dep <- arrow::read_parquet(file.path(IN_DIR, "tm_nom_dep.parquet")) 
# Mort
death_df <- arrow::read_parquet(file.path(IN_DIR, "tm_mort.parquet"))
# NIM
nim_gccsa <- arrow::read_parquet(file.path(IN_DIR, "tm_nim_gccsa.parquet"))

#study
contstudy_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_to_cont_study_higher_level.parquet"))
study_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_to_study_tertiary_non_continuous.parquet"))
studynotarget_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_to_study_tertiary_no_target.parquet"))
loefoecount_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_to_study_tertiary_loe_foe_count_simulation.parquet"))
loefoeprop_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_to_study_tertiary_loe_foe_simulation.parquet"))

#assign loefoe
loefoe_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_dist_loe_foe_study_2021_highestloe.parquet"))
loefoeocc_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_dist_loe_foe_study_occ_digit_various.parquet"))
loefoeunempnilf_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_dist_loe_foe_study_2021_unempnilf_combined.parquet"))

#study completions
#HE
comphe_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_study_completion_he.parquet"))
comphe2_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_study_completion_he_no_state.parquet"))
comphe3_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_study_completion_he_no_state_int.parquet"))
comphe4_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_study_completion_he_no_state_int_foe.parquet"))
#VET
compvet_df <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_study_completion_vet.parquet"))
#study duration
maxcomp_df <- arrow::read_parquet(file.path(file.path(FEATURE_DIR, "feat_study_completion_duration.parquet")))
#school completion
completion_yr12_2021 <- arrow::read_parquet(file.path(FEATURE_DIR, "feat_study_completion_yr12.parquet"))

#graduate to employment
grademp_df <- arrow::read_parquet(file.path(IN_DIR, "tm_prop_study_to_emp_loefoe_lfs_occ4.parquet"))

# lf status tm
lf_flow_df <- arrow::read_parquet(file.path(IN_DIR, "tm_lf_status_flow_disag.parquet"))

# COMMAND ----------

# raw for each person
entrants_df <-  new_entrants |> uncount(pop)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Simulation

# COMMAND ----------

# base_df <- arrow::read_parquet(file.path(TEST_DIR, "2025_03_31_test_pop_sim", "2025-03-31_test_pop_sim_2029.parquet")) |> 
#               filter(flag_death ==0 , flag_dep == 0) |>
#               rename(gccsa_code = curr_gccsa_code) |>
#               select(-prev_gccsa_code, -flag_death, -flag_dep)

#  base_df <- arrow::read_parquet(file.path(IN_DIR, "spop_census_2021_visa_countdown.parquet")) |>
#           mutate(id = sprintf("uid-%04d-%08d", year, row_number())) |> # create unique id
#           relocate(year)   

# COMMAND ----------

# years of sim
sim_year = 2022:2024
sim_year

# COMMAND ----------

# sim <- supply_sim(base_df, sim_year)

# COMMAND ----------

# use cluster cpu's 
n_cores <-  parallel::detectCores()-2
set_collapse(nthreads = n_cores, mask = "%in%")
tic()
lst_out <- vector(mode = "list", length = length(sim_year))
tictoc::tic()
for(i in 1:length(sim_year)){
  tictoc::tic(paste0("Simulation time for year ", sim_year[i]))
  cat("Simulating Population for year:", sim_year[i], "\n")
  pop_df_tmp <- pop_fun(base_df)
  cat("Simulating NOM for year:", sim_year[i], "\n")
  nom_df_tmp <- nom_fun(pop_df_tmp)
  cat("Simulating mortalities for year:", sim_year[i], "\n")
  mort_df_tmp <- mort_fun(nom_df_tmp)
  cat("Simulating Departures for year:", sim_year[i], "\n")
  dep_df_tmp <- dep_fun(mort_df_tmp)
  cat("Simulating NIM for year:", sim_year[i], "\n")
  nim_df_tmp <- nim_fun(dep_df_tmp)

#uncomment below for education transitions
  #education functions
  #studycd_df_tmp <- studycd_fun(nim_df_tmp)
  #grademp_df_tmp <- grademp_fun(studycd_df_tmp) #assign lf status and occ to recent grads recentgrad_flag == 1
  cat("Simulating labour forces status flow for year:", sim_year[i], "\n")
  lf_status_df_tmp <- lf_status_fun(nim_df_tmp)

  # #education functions continued
  # schoolcompcd_df_tmp <- schoolcompcd_fun(grademp_df_tmp)
  # #stuvisaenrol_df_tmp <- stuvisaenrol_fun(schoolcompcd_df_tmp) #assigning arrival loe 6 in nom_fun
  # contstudy_df_tmp <- contstudy_fun(schoolcompcd_df_tmp)
  # newstudy_df_tmp <- newstudynotarget_fun(contstudy_df_tmp) #testing with no target
  # assignloefoe_df_tmp <- assignloefoe_fun(newstudy_df_tmp)
  # passfailtertiary_df_tmp <- passfailtertiary_fun(assignloefoe_df_tmp)
  # studyduration_df_tmp <- studyduration_fun(passfailtertiary_df_tmp)
  # learningjourney_df_tmp <- learningjourney_fun(studyduration_df_tmp)

   lst_out[[i]] <-lf_status_df_tmp 

    #learningjourney_df_tmp %>% arrow::write_parquet(file.path(TEST_DIR,"2025_03_31_test_pop_sim", paste0(Sys.Date(),"_test_pop_sim_",sim_year[i],".parquet")))

   base_df <- lf_status_df_tmp |> 
              filter(flag_death ==0 , flag_dep == 0) |>
              rename(gccsa_code = curr_gccsa_code, lf_status = curr_lf_status) |>
              select(-prev_gccsa_code, -flag_death, -flag_dep, -prev_lf_status)
                          
  rm( list = ls(pattern="tmp"))
  gc()
  tictoc::toc()
}
tm <- tictoc::toc(log = TRUE, quiet = TRUE)
tm_msg(tm$callback_msg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save results

# COMMAND ----------

lst_out |> bind_rows() |>
arrow::write_dataset(file.path(IN_DIR, "pop_sim_2022_24"), partitioning = c("year"), existing_data_behavior = "overwrite")

# COMMAND ----------

