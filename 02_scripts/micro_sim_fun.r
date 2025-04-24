# Databricks notebook source
# MAGIC %md ### Micro simulation main functions

# COMMAND ----------

# MAGIC %md
# MAGIC `supply_sim(data, sim_year)`
# MAGIC
# MAGIC input values:
# MAGIC - data: data frame type or tibble representing base population
# MAGIC - sim_year: numeric vector, representing years to simulate
# MAGIC
# MAGIC output values:
# MAGIC - list of simulated data frames.

# COMMAND ----------

supply_sim <- function(data, sim_year){
# Checks  
if(!is.data.frame(data)) stop("data must be of data frame type")  
if(nrow(data) < 1) stop("data must not be empty")
if(length(sim_year) < 1) stop("sim_year must be a vector with one or more elements")

lst_out <- vector(mode = "list", length = length(sim_year))

 for(i in 1:length(sim_year)){
   tictoc::tic(paste0("total simulation time for year ", sim_year[i]))

  pop_df_tmp <- pop_fun(data, i)
  nom_df_tmp <- nom_fun(pop_df_tmp, i)
  mort_df_tmp <- mort_fun(nom_df_tmp, i)
  dep_df_tmp <- dep_fun(mort_df_tmp, i)
  nim_df_tmp <- nim_fun(dep_df_tmp, i)

  #  lst_out[[i]] <- nim_df_tmp 

 base_df <- nim_df_tmp |> 
              filter(flag_death == 0 , flag_dep == 0) |>
              rename(gccsa_code = curr_gccsa_code) |>
              select(-prev_gccsa_code, -flag_death, -flag_dep)
              
  rm( list = ls(pattern="tmp"))
  gc()
  tictoc::toc()
}
return(lst_out)
}

# COMMAND ----------

pop_fun <- function(df){
# age + 1 and year + 1 , assign id
pop_df <- df |>
            mutate(year = year + 1,
                  age = age + 1,
                  age = ifelse(age >100, 100, age), # collapse presons 100+ into 100
                  visa_countdown = visa_countdown-1,
                 # id = sprintf("base%04d", row_number())
                  ) |>
            rowbind(entrants_df |>
                      filter(year == sim_year[i]) |>
                      mutate(
                            age = ifelse(age >100, 100, age), # collapse presons 100+ into 100
                            #id = sprintf("new-%04d-%08d", year, row_number()),
                            citizenship = "australian citizen",
                            visa_type = "not applicable",
                            stup = "studying",
                            curr_loe_code = "6",
                            curr_foe_code = "1200",
                            learning_journey = "initial"
                            ),
            fill=TRUE
            ) |>
            mutate(citizenship = ifelse(citizenship != "australian citizen" & visa_countdown == 0 & citz_flag == 1, "australian citizen", citizenship),
                  visa_type = ifelse(visa_type != "not applicable" & visa_countdown == 0 & citz_flag == 1, "not applicable", visa_type),
                  visa_countdown = ifelse(visa_countdown == 0 & citz_flag == 1, NA, visa_countdown),
                  citz_flag = ifelse(is.na(visa_countdown), 0, citz_flag),
                  # if id not exist create new id
                  id = ifelse(is.na(id), sprintf("uid-%04d-%08d", year, row_number()), id)) |>
            relocate(id)
return(pop_df)
}

# COMMAND ----------

# nom
nom_fun <- function(df){
nom_df <- df |> 
          rowbind(arr_df |> 
                   filter(year == sim_year[i]) |>
                    #mutate(id = sprintf("arr-%04d-%08d", year, row_number()))
                    mutate(flag_arr = 1,
                           year_arr = year, # added for Charles to track when new arrivals arrived
                           
                           #all temp_stu visa holders have "studying" status
                            #assign loe 6 foe 1200 for under 18 years old
                            #assume all arrivals pass loe 6, countdown is years to graduating i.e. 18 years old
                            #subsequent study functions will assign loe/foe for students aged 18 and over
                            stup = ifelse(age < 18, "studying", stup),
                            curr_loe_code = ifelse(age < 18, "6", NA),
                            curr_foe_code = ifelse(age < 18, "1200", NA),
                            completion_flag = ifelse(age < 18, "P", NA),
                            curr_countdown = ifelse(age < 18, 18 - age, NA)),
                    fill=TRUE
                    ) |>
           mutate(id = ifelse(is.na(id), sprintf("uid-%04d-%08d", year, row_number()), id))
return(nom_df)
}

# COMMAND ----------

mort_fun <- function(df){
            set.seed(123)
           # df <- df |> select(id, year, gccsa_code, gender, age)
            death_ids_tmp <- df |>
                            join(
                                death_df |>
                                filter(year == sim_year[i]),
                                on = c("year", "gccsa_code", "gender", "age"),
                                how = "left",
                                verbose = 0
                                ) |>
                    group_by(year, gccsa_code, gender, age) |>
                    mutate(size_group = n(),
                    # due to larger deaths than group size this has to be constrained. the cause is starting census starting pop don't match treasury        
                          deaths = ifelse(deaths > size_group, size_group, deaths) )|> 
                  nest() |>          # nest data
                  # sample using id values and (unique) deaths value
                  mutate(v = map(data, ~sample_n(data.frame(id=.$id), unique(.$deaths)))) |>  
                  unnest(v) |>
                  pull(id)

            mort_df <- df |>
                        mutate(flag_death = if_else(id %in% death_ids_tmp, 1, 0)) # flag death 180,176 
            return(mort_df)
            rm(list = ls(pattern ="tmp"))
}

# COMMAND ----------

 # from devops
dep_fun <- function(df){
  # take samples
  set.seed(123)
  dep_ids_tmp <- df |>
                  fsubset(flag_death == 0 & (visa_countdown <= 0 | is.na(visa_countdown)) & citz_flag == 0) |>
                  join(
                    nom_dep |> 
                    filter(year == sim_year[i]),
                    on = c("year", "gccsa_code", "gender", "age", "visa_type"),
                    how ="left",
                    verbose = 0
                  ) |> 
                  mutate(count = ifelse(is.na(count), 0 , count)) |>
                  group_by(year, gccsa_code, gender, age, visa_type) |>
                  mutate(size_group = n(),
                    # due to larger departure than group size this has to be constrained - any excess departures will be randomly drawn again from leftover visa_coutdown = 0 migrants and all au citizens
                       dep = ifelse(count > size_group, size_group, count))|> 
                  nest() |>
                  # sample using id values and (unique) deaths value
                mutate(v = map(data, ~sample_n(data.frame(id = .$id), unique(.$dep)))) |>  
                unnest(v) |>
                pull(id)
  
  # add departure flag
  dep_df <- df |>
              mutate(flag_dep = if_else(id %in% dep_ids_tmp, 1, 0))

  # calculate excess departures by year, gender, age, gccsa
  excess_dep_tmp <-  df |>
                fsubset(flag_death == 0 & (visa_countdown <= 0 | is.na(visa_countdown)) & citz_flag == 0) |>
                group_by(year, gccsa_code, gender, age, visa_type) |>
                summarise(size_group = n(), .groups = "drop") |>
                join(
                  nom_dep |> 
                  filter(year == sim_year[i]),
                  on = c("year", "gccsa_code", "gender", "age", "visa_type"),
                  how ="left",
                  verbose = 0
                ) |> 
                mutate(count = ifelse(is.na(count), 0 , count)) |>
                mutate(gap = ifelse(count > size_group, count-size_group, 0)) |>
                select(-count, -size_group, -visa_type) |>
                # also add departures for some groups that are in the target but not in the population
                bind_rows(nom_dep %>% 
                          filter(year == sim_year[i]) %>% 
                          join(df |>
                                    fsubset(flag_death == 0 & (visa_countdown <= 0 | is.na(visa_countdown)) & citz_flag == 0) |>
                                    group_by(year, gccsa_code, gender, age, visa_type) |> 
                                    summarise(size_group = n(), .groups = "drop"),
                                    on = c("year", "gccsa_code", "gender", "age", "visa_type"),
                                    how ="left",
                                    verbose = 0
                                    ) %>% 
                          filter(is.na(size_group), count!=0) |>
                          select(-size_group, - visa_type) |>
                          rename(gap = count)) |>
                group_by(year, gccsa_code, gender, age) |>
                summarise(gap = sum(gap), .groups = "drop") |>
                filter(gap != 0) |>
                ungroup()

  # take samples for excess departures  
  dep_ids_tmp2 <- dep_df |>
                fsubset(flag_dep == 0 & flag_death == 0 & (visa_countdown <= 0 | is.na(visa_countdown)) & citz_flag == 0) |>
                join(
                  excess_dep_tmp, 
                  on = c("year", "gccsa_code", "gender", "age"),
                  how ="left",
                  verbose = 0
                ) |> 
                filter(!is.na(gap)) |>
                group_by(year, gccsa_code, gender, age) |>
                nest() |>
                # sample using id values and (unique) dep value
              mutate(v = map(data, ~sample_n(data.frame(id = .$id), unique(.$gap)))) |>  
              unnest(v) |>
              pull(id)
  
  dep_df <- dep_df |>
            mutate(flag_dep = if_else(flag_dep ==1, 1, if_else(id %in% dep_ids_tmp2, 1, 0))) 

  return(dep_df)
  rm(list = ls(pattern ="tmp"))

}

# COMMAND ----------

nim_fun <- function(df){
 df_tmp <- df |>
        fsubset(flag_death == 0 & flag_dep == 0) |> 
        fselect(id, year, gccsa_code, gender, age)

col_names <- c("year", "gccsa_code", "gender", "age")
df_tmp$group_id <- do.call(paste0, c(df_tmp[,col_names])) 

nim_dep_tmp <- nim_gccsa |>
          filter(year == sim_year[i]) |>
          select(year, gccsa_code, gender, age, dest_gccsa, dep = value) |>
          group_by(year, gccsa_code, gender, age) |>
          mutate(group_id = paste0(year, gccsa_code, gender,age)) |>
          ungroup() |>
          filter(group_id %in% df_tmp$group_id) |>
          select(group_id, gccsa_code, dest_gccsa, dep) 

# # update depture table to include those who don't change location
dep_tmp <-  nim_dep_tmp |> 
                join(
                  df_tmp |>
                  group_by(group_id) |>
                  summarise(n = n()) |>
                  join(nim_dep_tmp |>
                  group_by(group_id) |>
                  summarise(tlt_dep = sum(dep)) ,
                  on = "group_id", how = "left",
                  verbose = 0),
                  on = "group_id", how = "left",
                  verbose = 0
                ) |>
                mutate(dep = ifelse(gccsa_code == dest_gccsa & dep == 0, n-tlt_dep, dep)) |>
                select(-n, -tlt_dep)

gr_id_tmp = df_tmp|> distinct(group_id) |> pull()
lst_tmp = vector(mode = "list", length = length(gr_id_tmp))
for(i in 1:length(gr_id_tmp)){
   gr_tmp_id = df_tmp |> filter(group_id == gr_id_tmp[i])
   gr_tmp_dep = dep_tmp  |> filter(group_id == gr_id_tmp[i])
   lst_tmp[[i]] = rsplit(gr_tmp_id  ,sample(rep(gr_tmp_dep$dest_gccsa, times= gr_tmp_dep$dep))) |> 
                tibble::enframe("curr_gccsa_code") |> 
                unnest(value)
}
out_tmp <- lst_tmp |> rowbind() 
nim_df = df |>
        join(out_tmp |> 
        fselect(-year, -age, -gender, -gccsa_code, -group_id), on = "id", how = "left", verbose = F) |>
        fmutate(curr_gccsa_code = if_else(flag_dep == 1 | flag_death ==1, gccsa_code, curr_gccsa_code)) |>
        rename(prev_gccsa_code = gccsa_code) |>
        relocate(curr_gccsa_code, .before= prev_gccsa_code)

return(nim_df)
rm(list = ls(pattern ="tmp"))

}

# COMMAND ----------

# nim_fun <- function(df){
#   tmp_df <- df |>
#         filter(flag_death == 0, flag_dep == 0) |> 
#         select(id, year, gccsa_code, gender, age) |>
#         group_by(year, gccsa_code, gender, age) |>
#         mutate(group_id = paste0(year, gccsa_code, gender,age)) |>
#         ungroup() |>
#         select(id, group_id, gccsa_code) |>
#         arrange(group_id)

#   nim_dep_tmp <- nim_gccsa |>
#           filter(year == sim_year[i]) |>
#           select(year, gccsa_code, gender, age, dest_gccsa, dep = value) |>
#           group_by(year, gccsa_code, gender, age) |>
#           mutate(group_id = paste0(year, gccsa_code, gender,age)) |>
#           ungroup() |>
#           filter(group_id %in% tmp_df$group_id) |>
#           select(group_id, gccsa_code, dest_gccsa, dep) |>
#           arrange(group_id)
#   id_gr_tmp <- unique(tmp_df$group_id)
#   id_dep_tmp <- nim_dep_tmp |> distinct(gccsa_code,dest_gccsa, dep)

#   lst_tmp = list()
#   out_tmp <- list()    
#   for(i in 1: length(id_gr_tmp)){
#   tmp_group <- tmp_df |> 
#              filter(group_id == id_gr_tmp[i]) 
#   tmp_dep = nim_dep_tmp|> 
#     filter(group_id == id_gr_tmp[i]) 

#     for(j in 1:nrow(tmp_dep)) {
#       set.seed(123)
#       lst_tmp[[j]] <- tmp_group |> 
#         sample_n(size = tmp_dep$dep[j]) |> 
#         mutate(dest_gccsa = tmp_dep$dest_gccsa[j])
     
#       tmp_group <- tmp_group |> filter(!id %in% lst_tmp[[j]]$id)
#     }
#   dt_tmp = lst_tmp |> bind_rows()
#   out_tmp[[i]] <- dt_tmp
# }  
# nim_id_tmp <- out_tmp |> 
#               bind_rows() |>
#               select(id, dest_gccsa)
   
# nim_df <- df |>
#           left_join(nim_id_tmp,
#           join_by(id)
#           ) |>
#           mutate(dest_gccsa = ifelse(is.na(dest_gccsa),gccsa_code, dest_gccsa )) |>
#           rename(curr_gccsa_code = dest_gccsa, prev_gccsa_code = gccsa_code) |>
#           relocate(curr_gccsa_code, .before = prev_gccsa_code)

# return(nim_df)
# rm(list = ls(pattern ="tmp"))

# }

# COMMAND ----------

# function to print msg
tm_msg <- function(x) { # x is tictoc::toc object
  x <- readr::parse_number(x)
  if(x < 60){
    cat("\n # Total Simulation time # \n", x ," seconds")
  } 
  else if (x >= 60 & x < 3600){
    cat("\n # Total Simulation time # \n", round(x/60,3) , " minutes")
  } else {
   cat("\n # Total Simulation time # \n", round(x/3600,3) , " hours")
  }
 
}

# COMMAND ----------

# sampling 
sample_flow <- function(data) {
  set.seed(123)
  n <- unique(data$sample)
  # random draw
  sample_n(data, size = n, replace = FALSE)
}

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

# Function to assign employed individuals by group
emp_by_group <- function(.x, grp_id) {
  ## Flow to employed
  # emp_smpl <- grp_flow |> 
  #   filter(grp_id == !!grp_id, flow_to == "employed") |>
  #   mutate(
  #     pop = ifelse(is.na(pop), 0, pop),
  #     chk = ifelse(smpl > pop, 1, 0)) 
  emp_smpl <- .x|> 
    filter(flow_to == "employed") |>
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

unemp_by_group <- function(.x, grp_id) {
unemp_smpl <-  .x |> 
           filter(flow_to == "unemployed") |>
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

# lf status function
lf_status_fun <- function(df){
  pop_df_tmp <- df |>
          fsubset(flag_death == 0 & flag_dep == 0) |> 
          select(id, year, gender, age, gccsa_code = curr_gccsa_code, lf_status) |>
          #select(id, year, gender, age, gccsa_code, lf_status) |>
          mutate(grp_id = paste0(year, gccsa_code, gender, age))
  grp_pop_tmp = pop_df_tmp |>
          group_by(grp_id, lf_status) |> 
          summarise(pop = n(), .groups='drop') 
  lf_flow_tmp <- lf_flow_df |>
            filter(year ==!!sim_year[i]) |>
            mutate(grp_id = paste0(year,gccsa_code, gender, age)) |> 
            select(grp_id, lf_status, flow_to, lf_target, smpl)
  grp_flow_tmp <- grp_pop_tmp |> 
    left_join(
    lf_flow_tmp, join_by(grp_id, lf_status)
    )
  ## First Flow to emplyement
  # correct the target (sample) size with the population 
  emp_flow_tmp <- grp_flow_tmp |>
        group_split(grp_id) |>
        map_dfr(~ emp_by_group(.x, .x$grp_id[1]))
  # Expand the counts into individual assignments
  expanded_counts_emp_tmp <- emp_flow_tmp |> uncount(smpl)
  emp_df_tmp <- pop_df_tmp |> 
        group_by(grp_id) %>%
        group_split() %>%
          map_df(~ {
            #print(.x$grp_id[1])
            # Get population and assignment needs for this group
            pop_ids <- pop_df_tmp %>% filter(grp_id == .x$grp_id[1]) %>% pull(id) %>% sample()
            assigns <- expanded_counts_emp_tmp %>% filter(grp_id == .x$grp_id[1])         
            # Assign IDs to destinations
            tibble(
              id = pop_ids[seq_len(nrow(assigns))],
              grp_id = .x$grp_id[1],
              flow_to = assigns$flow_to
            )
          })
  ## second Flow to unemplyement
  # update grp_flow to exlucde emp ids
  pop_size_tmp <- pop_df_tmp |> 
      filter(!id %in% emp_df_tmp$id) |>
      group_by(grp_id, lf_status) |>
      summarise(pop = n(), .groups= 'drop')
  grp_flow_unemp_tmp <-  grp_flow_tmp |> 
           filter(flow_to == "unemployed") |>
            select(-pop) |>
            left_join(pop_size_tmp, join_by(grp_id, lf_status))
  unemp_flow_tmp <-  grp_flow_unemp_tmp |>
        group_split(grp_id) |>
        map_dfr(~ unemp_by_group(.x, .x$grp_id[1]))
  # Expand the counts into individual assignments
  expanded_counts_unemp_tmp <- unemp_flow_tmp |> uncount(smpl)
  unemp_df_tmp = pop_df_tmp |> 
        filter(!id %in% emp_df_tmp$id) |>
        group_by(grp_id) %>%
        group_split() %>%
          map_df(~ {
            #print(.x$grp_id[1])
            # Get population and assignment needs for this group
            pop_ids <- .x %>% filter(grp_id == .x$grp_id[1]) %>% pull(id) %>% sample()
            assigns <- expanded_counts_unemp_tmp %>% filter(grp_id == .x$grp_id[1])         
            # Assign IDs to destinations
            tibble(
              id = pop_ids[seq_len(nrow(assigns))],
              grp_id = .x$grp_id[1],
              flow_to = assigns$flow_to
            )
          })
  # put grp together emp, unemp and nilf
  grp_df_tmp <- bind_rows(emp_df_tmp, unemp_df_tmp)
  # out_tmp <- pop_df |> 
  #   left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
  #   mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))
   out_tmp <- pop_df_tmp |> 
    join(grp_df_tmp |> fselect(id, flow_to), on = "id", how = "left", verbose = F) |>
    fmutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))
  
  # lf_status_df <- df |>
  #           left_join(out_tmp |> select(id, flow_to), join_by(id)) |>
  #           mutate(flow_to = ifelse(is.na(flow_to), "not applicable", flow_to)) |>
  #           rename(prev_lf_status = lf_status, curr_lf_status = flow_to)
   lf_status_df <- df |>
            join(out_tmp |> fselect(id, flow_to), on = "id", how = "left", verbose = F) |>
            fmutate(flow_to = ifelse(is.na(flow_to), "not applicable", flow_to)) |>
            rename(prev_lf_status = lf_status, curr_lf_status = flow_to)

  return(lf_status_df )
  rm(list = ls(pattern ="tmp"))
}

# COMMAND ----------

flow_fun <- function(pop_df, grp_flow, grp_id){

### Flow to employed
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

emp_lst = list(length = nrow(emp_smpl))
for(i in 1: nrow(emp_smpl)){
  emp_lst[[i]] <- pop_df |> 
                  filter(grp_id == !!grp_id, lf_status == emp_smpl$lf_status[i]) |>
                  sample_n(size = emp_smpl$smpl[i]) |>
                  mutate(flow_to = !!emp_smpl$flow_to[i])
                  }
emp_df <- emp_lst |> bind_rows()

### Flow to unemployed
# pop size have to be updates
pop_size = pop_df |> 
      filter(grp_id == !!grp_id, !id %in% emp_df$id) |>
      group_by(grp_id, lf_status) |>
      summarise(pop = n(), .groups= 'drop')

unemp_smpl <- grp_flow |> 
           filter(grp_id == !!grp_id, flow_to == "unemployed") |>
            select(-pop) |>
            left_join(pop_size, join_by(grp_id, lf_status)) |>
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
unemp_lst = list(length = nrow(unemp_smpl))
for(i in 1: nrow(unemp_smpl)){
  unemp_lst[[i]] <- pop_df |> 
                  filter(grp_id == !!grp_id, lf_status == unemp_smpl$lf_status[i], , !id %in% emp_df$id) |>
                  sample_n(size = unemp_smpl$smpl[i]) |>
                  mutate(flow_to = !!unemp_smpl$flow_to[i])
                  }
unemp_df <- unemp_lst |> bind_rows()
# put grp together emp, unemp and nilf
grp_df_tmp <- bind_rows(emp_df, unemp_df)
grp_df <- pop_df |> 
    filter(grp_id == !!grp_id) |>
    left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
    mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))

return(grp_df)
}

# COMMAND ----------

# grp_flow <- function(pop_df, state_flow, state_pop, grp_id){
 
#   # targets
#   trg_value <- state_flow |> 
#     filter(grp_id == !!grp_id, lf_status == flow ) |> 
#     pull(value_flow)
  
#   trg_name <- state_flow |> 
#     filter(grp_id == !!grp_id, lf_status == flow ) |> 
#     pull(flow)
  
#   names(trg_value) <- trg_name
  
#   trg_df <- state_pop |> 
#     filter(grp_id == !!grp_id) |> 
#     left_join(
#       state_flow |> 
#         filter(grp_id == !!grp_id ),
#       join_by(grp_id, lf_status)
#     ) |> 
#     group_by(grp_id, flow) |>
#     mutate(
#       #prop = value_flow/sum(value_flow),
#       prop = ifelse(is.nan(value_flow/sum(value_flow)),0, value_flow/sum(value_flow)),
#       smpl = case_when(
#         flow == "employed" ~ prop * trg_value['employed'],
#         flow == "unemployed" ~ prop * trg_value['unemployed'],
#         flow == "nilf" ~ prop * trg_value['nilf'],
#       ),
#       smpl = round(smpl,0)
#     ) |>
#     ungroup() |> 
#     arrange(flow)
  
#   # Flow to employed
#   flow_to_name <- "employed"
#   emp_smpl <- trg_df |> 
#     filter(flow == !!flow_to_name) |>
#     mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) #|> # this needs to be check       
#     #pull(smpl)
  
#   emp_lst = list()
#   # for(i in 1:length(emp_smpl)){
#   #   emp_lst[[i]] <- pop_df |> 
#   #     filter(grp_id == !!grp_id, lf_status == trg_name[i]) |> 
#   #     sample_n(size = emp_smpl[i]) |>
#   #     mutate(flow_to = !!flow_to_name)
#   # }
#     for(i in 1:nrow(emp_smpl)){
#     emp_lst[[i]] <- pop_df |> 
#        filter(grp_id == !!grp_id, lf_status == emp_smpl$lf_status[i]) |>
#        sample_n(size = emp_smpl$smpl[i]) |>
#       mutate(flow_to = !!flow_to_name)
#   }
#   emp_df <- bind_rows(emp_lst)
#   #return(emp_df)
#   # Flow to unemployed
#   flow_to_name <- "unemployed"
#   # unemp_smpl <- trg_df |> 
#   #   filter(flow == !!flow_to_name) |>
#   #   mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) |> # this needs to be check       
#   #   pull(smpl)
#   unemp_smpl <- trg_df |> 
#     filter(flow == !!flow_to_name) |>
#     mutate(smpl = ifelse(smpl > pop, round(pop * prop, 0), smpl)) |>
#     select(grp_id, lf_status, smpl)

# # pop size
#  pop_size = pop_df |> 
#       #filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% dat$id) 
#       filter(grp_id == !!grp_id, !id %in% emp_df$id) |>
#       group_by(grp_id, lf_status) |>
#       summarise(pop = n(), .groups= 'drop')

# # Check point, sample size is smaller than avaiable data to sample from
# chk_df = pop_size |>
#         left_join(unemp_smpl, join_by(grp_id, lf_status)) |>
#         mutate(chk = ifelse(smpl > pop, 0, 1))

# if(any(chk_df$chk ==0)) {
#   unemp_smpl <- smpl_check(chk_df, target= round(trg_value[names(trg_value) == flow_to_name],0)) |> pull(smpl)
# } else {
#   unemp_smpl <- unemp_smpl |> pull(smpl)
# }

#   unemp_lst = list()
#   for(i in 1:length(unemp_smpl)){
#     tmp <- pop_df |> 
#       filter(grp_id == !!grp_id, lf_status == trg_name[i], !id %in% emp_df$id)
#     # Check if no data left to sample from
#     if(nrow(tmp) >=  unemp_smpl[i]) {
#     unemp_lst[[i]] <- tmp |> sample_n(size = unemp_smpl[i]) |>
#                       mutate(flow_to = !!flow_to_name)
#                       } else {
#                         unemp_lst[[i]]<- tmp |> sample_n(size = 0) |>
#                         mutate(flow_to = !!flow_to_name)
#                         }
#                         }
#   unemp_df <- bind_rows(unemp_lst)
  
#   # put grp together emp, unemp and nilf
#   grp_df_tmp <- bind_rows(emp_df, unemp_df)
#   grp_df <- pop_df |> 
#     filter(grp_id == !!grp_id) |>
#     left_join(grp_df_tmp |> select(id, flow_to), join_by(id)) |>
#     mutate(flow_to = ifelse(is.na(flow_to), "nilf", flow_to))
  
#   return(grp_df)
  
# }

# COMMAND ----------

#function to countdown study, flag recent graduates and reallocate qualification details for education completions

studycd_fun <- function(df){
  cd_tmp <- df %>%
  mutate(curr_countdown = ifelse(curr_countdown > 0, curr_countdown - 1, curr_countdown),
         commencement_flag = NA) #clear any previous commencement flags, flags are updated when a new study is commenced in later functions

  #remove study status for failed student, and clear all current study info
fail_tmp <- cd_tmp %>%
  filter(curr_countdown == 0, completion_flag == "F") %>%
  mutate(
    stup = "not_studying",
    curr_loe_code = NA,
    curr_foe_code = NA,
    curr_countdown = NA,
    completion_flag = NA,
    recentgrad_flag = 0,
    recentgrad_loe = NA,
    recentgrad_foe = NA
  )
  #reassign loe/foe info for those who passed
pass_tmp <- cd_tmp %>%
  filter(curr_countdown == 0, completion_flag == "P") %>%
  mutate(
    new_highest_loe_flag = ifelse(as.numeric(curr_loe_code) <= as.numeric(highest_loe_code) | is.na(highest_loe_code), 1, 0)
  ) %>%
  mutate(
    recentgrad_flag = 1,
    recentgrad_loe = curr_loe_code,
    recentgrad_foe = curr_foe_code,
    other_qual = ifelse(new_highest_loe_flag == 1, paste(other_qual, highest_loe_code, sep=", "), paste(other_qual, curr_loe_code,sep=", ")),highest_loe_code = ifelse(new_highest_loe_flag == 1, curr_loe_code, highest_loe_code),
    highest_foe4_code = ifelse(new_highest_loe_flag == 1, curr_foe_code, highest_foe4_code),
    highest_foe2_code = ifelse(new_highest_loe_flag == 1, substr(curr_foe_code, 1, 2), highest_foe2_code),
    stup = "not_studying",
    curr_loe_code = NA,
    curr_foe_code = NA,
    curr_countdown = NA,
    completion_flag = NA
  ) %>%
  mutate(other_qual = sapply(strsplit(other_qual, ","), function(x) paste(sort(as.numeric(x)), collapse = ","))) %>%
  select(-new_highest_loe_flag)
  
  #combine pass and fail students
  combined_tmp <- bind_rows(pass_tmp, fail_tmp)

  studycd_df <- cd_tmp %>%
    anti_join(combined_tmp %>% select(id), by = c("id")) %>%
    mutate(recentgrad_flag = 0,
           recentgrad_loe = NA,
           recentgrad_foe = NA) %>%
    bind_rows(combined_tmp)

  return(studycd_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#function to assign new lf status and occ for recent graduates - recentgrad_flag == 1

grademp_fun <- function(df){
  grad_tmp <- df %>%
  filter(recentgrad_flag == 1) %>%
  group_by(recentgrad_loe, recentgrad_foe) %>%
  mutate(emp_prob = runif(n())) %>%
  ungroup() %>%
  left_join(grademp_df, by = join_by(recentgrad_loe == model_loe_code,
                                     recentgrad_foe == model_foe_code,
                                     emp_prob >= prev_cumulative_prop,
                                     emp_prob <= cumulative_prop)) %>%
  select(-c(emp_prob, lf_prop, cumulative_prop, prev_cumulative_prop)) %>%
  mutate(lfs_counter = ifelse(lf_status == new_lfs | is.na(new_lfs), lfs_counter + 1, 0),
         prev_any_anzsco = ifelse(anzsco4_code != new_anzsco4 & !is.na(new_anzsco4), prev_1yr_anzsco, prev_any_anzsco),
         prev_1yr_anzsco = ifelse(anzsco4_code != new_anzsco4 & !is.na(new_anzsco4), anzsco4_code, prev_1yr_anzsco),
         lf_status = ifelse(!is.na(new_lfs), new_lfs, lf_status),
         anzsco4_code = ifelse(!is.na(new_anzsco4), new_anzsco4, anzsco4_code)) %>%
  select(-c(new_anzsco4, new_lfs))

  grademp_df <- df %>%
    anti_join(grad_tmp %>% select(id), by = c("id")) %>%
    bind_rows(grad_tmp)

  return(grademp_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#school completion supplemetary fun - used in schoolcompcd_fun
school_sample_f <- function(data) {
  set.seed(123)
  total_rows <- nrow(data)
  prob <- unique(data$proportion)
  # number of rows to sample
  sample_size <- ceiling(total_rows * prob)
  # random draw
  sample_n(data, size = sample_size, replace = FALSE)
}

# COMMAND ----------

# function to add completion_flag and countdown for loe 6 (new entrants study)

schoolcompcd_fun <- function(df){
  newschool_tmp <- df %>%
  filter(curr_loe_code == "6", is.na(completion_flag)) %>%
  mutate(gccsa_join = as.character(ifelse(substr(curr_gccsa_code, 1, 1) %in% c("6", "7", "8"), substr(curr_gccsa_code, 1, 1), curr_gccsa_code))) %>%
  left_join(completion_yr12_2021, by = join_by(gender, gccsa_join == gccsa_code))

# yr12 sampling based on completion rate
school_sample_pass_tmp <- newschool_tmp %>%
  group_by(gender, gccsa_join) %>%
  group_modify(~ school_sample_f(.x)) %>%
  ungroup() %>%
  select(-c(proportion, gccsa_join)) %>%
  # add completion flag as pass for sampled, and countdown to 3
  mutate(completion_flag = "P",
         curr_countdown = 3)

school_sample_fail_tmp <- newschool_tmp %>% 
  anti_join(school_sample_pass_tmp, by=("id" ="id")) %>%
  select(-c(proportion, gccsa_join)) %>%
  # add completion flag as fail
  mutate(completion_flag = "F") %>%
  # add countdown for failed student - 0.3, 0.5, 0.2 split, dropping in yr 10, 11 and 12 respectively
  mutate(curr_countdown = sample(c(3,2,1), size=n(), replace = TRUE, prob=c(0.3, 0.5, 0.2)))

#combine pass/fail students
combined_tmp <- bind_rows(school_sample_pass_tmp, school_sample_fail_tmp)

schoolcompcd_df <- df %>%
    anti_join(combined_tmp %>% select(id), by = c("id")) %>%
    bind_rows(combined_tmp)

  return(schoolcompcd_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

# #function to add school study for new student visa arrivals

# stuvisaenrol_fun <- function(df) {
#   #all temp_stu visa holders have "studying" status
#   #those on student visa, assign loe 6 foe 1200 for under 18 years old
#   #assume school visa holders all pass loe 6, countdown is years to graduating i.e. 18 years old
#   #subsequent study functions will assign loe/foe for students aged 18 and over
#   stuvisa_tmp <- df %>%
#     filter(flag_arr_this_year == 1) %>%
#     mutate(stup = ifelse(age < 18, "studying", stup),
#            curr_loe_code = ifelse(age < 18, "6", NA),
#            curr_foe_code = ifelse(age < 18, "1200", NA),
#            completion_flag = ifelse(age < 18, "P", NA),
#            curr_countdown = ifelse(age < 18, 18 - age, NA)) %>%
#     select(-flag_arr_this_year)
  
#   stuvisaenrol_df <- df %>%
#     select(-flag_arr_this_year) %>%
#     anti_join(stuvisa_tmp %>% select(id), by = c("id")) %>%
#     bind_rows(stuvisa_tmp)
  
#   return(stuvisaenrol_df)
#   rm(list = ls(pattern = "tmp"))
# }

# COMMAND ----------

#function to assign higher level study for recent graduates who continue to study

contstudy_fun <- function(df){

  set.seed(123)

  nextstudy_df <- df %>%
    filter(recentgrad_flag == 1, recentgrad_loe %in% c("1", "3", "4", "5", "7")) %>%
    mutate(join_foe_code = paste0(substr(recentgrad_foe, 1, 2), "00")) %>%
    group_by(recentgrad_loe, join_foe_code) %>%
    mutate(cont_study_prob = runif(n())) %>%
    ungroup() %>%
    left_join(contstudy_df, by = join_by(recentgrad_loe == model_loe_code,
                                        join_foe_code == model_foe_code,
                                        cont_study_prob >= lower_prop,
                                        cont_study_prob <= upper_prop)) %>%
    mutate(curr_loe_code = next_loe_code,
          curr_foe_code = ifelse(is.na(next_loe_code), NA, recentgrad_foe),
          stup = ifelse(is.na(next_loe_code), "not_studying", "studying"),
          commencement_flag = ifelse(is.na(next_loe_code), NA, "continuing_tertiary")
          ) %>%
    select(-c(join_foe_code, cont_study_prob, next_loe_code, prop_next_loe, lower_prop, upper_prop))

  combined_df <- df %>%
    anti_join(nextstudy_df %>% select(id), by = c("id")) %>%
    bind_rows(nextstudy_df)

  return(combined_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#propensity to study (not continuous education journey)

newstudynotarget_fun <- function(df){

  #prep df
  pop_tmp <- df %>%
  mutate(age_group = case_when(
                          age < 25 ~ as.character(age),
                          age >= 25 & age <= 29 ~ "25-29",
                          age >= 30 & age <= 34 ~ "30-34",
                          age >= 35 & age <= 44 ~ "35-44",
                          age >= 45 & age <= 54 ~ "45-54",
                          age >= 55 & age <= 64 ~ "55-64",
                          age >= 65 ~ "65 and over"),
         y12_completion = ifelse(age >= 18 & (grepl("6", other_qual) | highest_loe_code == "6"), 1, 0))

  set.seed(123)

  #exclude new students who are continuing study from the commencement proportion (ignore student visas)
  contstudy_excl_tmp <- pop_tmp %>%
  mutate(cont_study = ifelse(stup == "studying" & is.na(curr_loe_code) & visa_type != "temp_stu", 1, 0)) %>%
  group_by(gender, age_group, curr_gccsa_code, lf_status, citizenship, y12_completion) %>%
  summarise(contstudy_count = sum(cont_study),
            cohort_count = n()) %>%
  ungroup() %>%
  mutate(cont_prop = contstudy_count / cohort_count) %>%
  select(-c(contstudy_count, cohort_count))

  studynotargetadjusted_tmp <- studynotarget_df %>%
    left_join(contstudy_excl_tmp, by = join_by(gender, age_group, citizenship, gccsa_code == curr_gccsa_code, lf_status, y12_completion)) %>%
    mutate(commence_prop = ifelse(is.na(cont_prop), commence_prop, commence_prop - cont_prop),
          commence_prop = ifelse(commence_prop < 0, 0, commence_prop)) %>%
    select(-cont_prop)

  #assign study status based on study probability
  newstudy_tmp <- pop_tmp %>%
    filter(stup == "not_studying") %>%
    group_by(gender, curr_gccsa_code, age_group, lf_status, citizenship, y12_completion) %>%
    mutate(study_prob = runif(n())) %>%
    ungroup() %>%
    inner_join(studynotargetadjusted_tmp,
              by = join_by(gender, curr_gccsa_code == gccsa_code, age_group, lf_status, citizenship, y12_completion, study_prob <= commence_prop)) %>%
    mutate(stup = ifelse(!is.na(commence_prop), "studying", stup),
           commencement_flag = ifelse(!is.na(commence_prop), "new_tertiary", stup)) %>%
    select(-c(commence_prop, study_prob, y12_completion))
  
  #join back to population
  newstudynotarget_df <- pop_tmp %>%
    anti_join(newstudy_tmp %>% select(id), by = c("id")) %>%
    bind_rows(newstudy_tmp)

  return(newstudynotarget_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#allocate loe foe for new students

assignloefoe_fun <- function(df){

  #get new students to assign loe/foe to
  student_tmp <- df %>%
    filter(stup == "studying", is.na(curr_loe_code)) %>%
    select(-c(curr_loe_code, curr_foe_code)) %>%
    mutate(highest_loe_group = ifelse(highest_loe_code %in% c(1, 3), 3,
                               ifelse(highest_loe_code %in% c(4, 5, 6, 7), 7, 8)))

  #generate random number within each cohort group
  set.seed(123)
  indprob_tmp <- student_tmp %>%
    group_by(age_group, citizenship, gender, lf_status, curr_gccsa_code, anzsco4_code, highest_loe_group) %>%
    mutate(study_prob = runif(n())) %>%
    ungroup()
  
########################################## employed students ##########################################
  indstudy_emp_tmp <- indprob_tmp %>%
  filter(lf_status == "employed") %>%
  left_join(loefoe_df %>% filter(lf_status == "employed"),
            join_by(age_group == age,
            gender,
            curr_gccsa_code == gccsa_code,
            citizenship,
            lf_status,
            anzsco4_code == occ4_code,
            highest_loe_group,
            study_prob >= prev_cumulative_prop,
            study_prob <= cumulative_prop)) %>%
  select(-c(prop_foe_loe, cumulative_prop, prev_cumulative_prop))

  #save successful joins
  indstudy_emp_joined_tmp <- filter(indstudy_emp_tmp, !is.na(loe_code))

####### NA treatment #######
  #save NA joins
  indstudy_emp_na_tmp <- filter(indstudy_emp_tmp, is.na(loe_code))
  #NA treatment - join by occ4
  indstudy_emp_na_occ4_tmp <- indstudy_emp_na_tmp %>%
  select(-c(loe_code, foe_24D_code)) %>%
  left_join(loefoeocc_df %>% filter(occ_digit == 4),
            join_by(lf_status,
            anzsco4_code == occ_code,
            highest_loe_group,
            study_prob >= prev_cumulative_prop,
            study_prob <= cumulative_prop)) %>%
  select(-c(prop_foe_loe, cumulative_prop, prev_cumulative_prop, occ_digit))
  #join by occ3
  indstudy_emp_na_occ3_tmp <- indstudy_emp_na_occ4_tmp %>%
  filter(is.na(loe_code)) %>%
  select(-c(loe_code, foe_24D_code)) %>%
  mutate(anzsco3_code = substr(anzsco4_code, 1, 3)) %>%
  left_join(loefoeocc_df %>% filter(occ_digit == 3),
            join_by(lf_status,
            anzsco3_code == occ_code,
            highest_loe_group,
            study_prob >= prev_cumulative_prop,
            study_prob <= cumulative_prop)) %>%
  select(-c(anzsco3_code, prop_foe_loe, cumulative_prop, prev_cumulative_prop, occ_digit))
  #join by occ2
  indstudy_emp_na_occ2_tmp <- indstudy_emp_na_occ3_tmp %>%
    filter(is.na(loe_code)) %>%
    select(-c(loe_code, foe_24D_code)) %>%
    mutate(anzsco2_code = substr(anzsco4_code, 1, 2)) %>%
    left_join(loefoeocc_df %>% filter(occ_digit == 2),
              join_by(lf_status,
              anzsco2_code == occ_code,
              highest_loe_group,
              study_prob >= prev_cumulative_prop,
              study_prob <= cumulative_prop)) %>%
    select(-c(anzsco2_code, prop_foe_loe, cumulative_prop, prev_cumulative_prop, occ_digit))
  #join by occ1
  indstudy_emp_na_occ1_tmp <- indstudy_emp_na_occ2_tmp %>%
    filter(is.na(loe_code)) %>%
    select(-c(loe_code, foe_24D_code)) %>%
    mutate(anzsco1_code = substr(anzsco4_code, 1, 1)) %>%
    left_join(loefoeocc_df %>% filter(occ_digit == 1),
              join_by(lf_status,
              anzsco1_code == occ_code,
              highest_loe_group,
              study_prob >= prev_cumulative_prop,
              study_prob <= cumulative_prop)) %>%
    select(-c(anzsco1_code, prop_foe_loe, cumulative_prop, prev_cumulative_prop, occ_digit))
###########################        
########################################## unemployed/nilf students ##########################################
  indstudy_unempnilf_tmp <- indprob_tmp %>%
    filter(lf_status != "employed") %>%
    left_join(loefoe_df %>% filter(lf_status != "employed") %>% select(-occ4_code),
              join_by(age_group == age,
              gender,
              curr_gccsa_code == gccsa_code,
              citizenship,
              lf_status,
              highest_loe_group,
              study_prob >= prev_cumulative_prop,
              study_prob <= cumulative_prop)) %>%
    select(-c(prop_foe_loe, cumulative_prop, prev_cumulative_prop))
  
  #save successful joins
  indstudy_unempnilf_joined_tmp <- filter(indstudy_unempnilf_tmp, !is.na(loe_code))

  ####### NA treatment #######
  #save NA joins
  indstudy_unempnilf_na_tmp <- filter(indstudy_unempnilf_tmp, is.na(loe_code))

  indstudy_unempnilf_na_joined_tmp <- indstudy_unempnilf_na_tmp %>%
  select(-c(loe_code, foe_24D_code)) %>%
  left_join(loefoeunempnilf_df,
            join_by(age_group == age,
            gender,
            curr_gccsa_code == gccsa_code,
            highest_loe_group,
            study_prob >= prev_cumulative_prop,
            study_prob <= cumulative_prop)) %>%
  select(-c(prop_foe_loe, cumulative_prop, prev_cumulative_prop))
###########################  
########################################## combine all data ##########################################
  #Combine all
  
    indstudy_all_tmp <- indstudy_emp_joined_tmp %>%
    bind_rows(indstudy_emp_na_occ4_tmp %>% filter(!is.na(loe_code))) %>%
    bind_rows(indstudy_emp_na_occ3_tmp %>% filter(!is.na(loe_code))) %>%
    bind_rows(indstudy_emp_na_occ2_tmp %>% filter(!is.na(loe_code))) %>%
    bind_rows(indstudy_emp_na_occ1_tmp) %>%
    bind_rows(indstudy_unempnilf_joined_tmp) %>%
    bind_rows(indstudy_unempnilf_na_joined_tmp) %>%
    rename(curr_loe_code = loe_code,
           curr_foe_code = foe_24D_code) %>%
    relocate(curr_loe_code, .after = other_qual) %>%
    relocate(curr_foe_code, .after = curr_loe_code) %>%
    select(-c(study_prob, highest_loe_group))

  assignloefoe_df <- df %>%
    filter(stup != "studying" | (stup == "studying" & !is.na(curr_loe_code))) %>%
    bind_rows(indstudy_all_tmp)
  
  return(assignloefoe_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#supplementary functions for assigning completion_flag and study countdown

VET_sample_f <- function(data) {
  set.seed(123)
  
  total_rows <- nrow(data)
  prob <- unique(data$comp_rate)
  # number of rows to sample
  sample_size <- ceiling(total_rows * prob)
  # random draw
  sample_n(data, size = sample_size, replace = FALSE)
}

HE_sample_f <- function(data) {
  set.seed(123)

  total_rows <- nrow(data)
  prob <- unique(data$avg_comp_rate)
  # number of rows to sample
  sample_size <- ceiling(total_rows * prob)
  # random draw
  sample_n(data, size = sample_size, replace = FALSE)
}

assign_skewed_fail <- function(n, max_dur) {

  set.seed(123)
  
  #range to assign
  values <- 1: max_dur

  # if(length(values)==1){
  #   return(rep(0,n))
  # }

  # ensure at least one of each value is assigned
  base_assignment <- values[1:(n %% length(values))]

  # Define skewed probability
  skew_prob <- dpois(values, lambda= 1) # poisson dsn - this allow more people get a small countdown rather than large one, logic being people tending to drop out early on rather than in later years during study (lambda = 1 allows drop in year 1 and 2 have the same probability)

  skew_prob <- skew_prob/sum(skew_prob) # normalise

  #remaining assignment
  remaining <- n - length(base_assignment) # adjust total sample size

  remaining_assignment <- sample(values, size = remaining, replace = TRUE, prob = skew_prob )
  
  # combine base and skewed assignment
  final_assignment <- c(base_assignment, remaining_assignment)

  #shuffle
  sample(final_assignment, length(final_assignment))
}

# COMMAND ----------

#create completion_flag for new students

passfailtertiary_fun <- function(df){
########################################## VET ##########################################
curr_vet_tmp <- df %>%
  filter(stup == "studying", is.na(completion_flag), curr_loe_code %in% c("4","5","7")) %>%
  mutate(curr_foe2_code = substr(curr_foe_code,1,2)) %>%
  left_join(compvet_df, by = c("curr_loe_code" = "loe_code",
                                    "curr_foe2_code" = "foe2_code"))

vet_sample_pass_tmp <- curr_vet_tmp %>%
  group_by(curr_loe_code, curr_foe2_code) %>%
  group_modify(~ VET_sample_f(.x)) %>%
  ungroup() %>%
  select(-c(age_group, comp_rate)) %>%
  # add completion flag as pass for sampled
  mutate(completion_flag = "P")

vet_sample_fail_tmp <- curr_vet_tmp %>% 
  anti_join(vet_sample_pass_tmp, join_by(id)) %>%
  select(-c(age_group, comp_rate)) %>%
  # add completion flag as fail
  mutate(completion_flag = "F")
########################################## HE ##########################################
curr_he_tmp <- df %>%
  filter(stup == "studying", is.na(completion_flag), curr_loe_code %in% c("1","3")) %>%
  mutate(state_code = as.numeric(substr(curr_gccsa_code, 1, 1)))

curr_he2_tmp <- curr_he_tmp %>%
  left_join(comphe_df, join_by(curr_loe_code == loe_code,
                              curr_foe_code == foe4_code,
                              age_group == age,
                              state_code,
                              gender,
                              citizenship)) %>%
  mutate(avg_comp_rate = ifelse(curr_foe_code == "1100", 0, avg_comp_rate))

#join again without state code
curr_he3_tmp <- curr_he2_tmp %>%
  filter(is.na(avg_comp_rate)) %>%
  select(-avg_comp_rate) %>%
  left_join(comphe2_df, join_by(curr_loe_code == loe_code,
                                curr_foe_code == foe4_code,
                                age_group == age,
                                gender,
                                citizenship)) %>%
  mutate(avg_comp_rate = ifelse(curr_foe_code == "1100", 0, avg_comp_rate))

#join again without state code, citizenship
curr_he4_tmp <- curr_he3_tmp %>%
  filter(is.na(avg_comp_rate)) %>%
  select(-avg_comp_rate) %>%
  left_join(comphe3_df, join_by(curr_loe_code == loe_code,
                                curr_foe_code == foe4_code,
                                age_group == age,
                                gender)) %>%
  mutate(avg_comp_rate = ifelse(curr_foe_code == "1100", 0, avg_comp_rate))

#join again without state code, citizenship, foe
curr_he5_tmp <- curr_he4_tmp %>%
  filter(is.na(avg_comp_rate)) %>%
  select(-avg_comp_rate) %>%
  left_join(comphe4_df, join_by(curr_loe_code == loe_code,
                              age_group == age,
                              gender)) %>%
  mutate(avg_comp_rate = ifelse(curr_foe_code == "1100", 0, avg_comp_rate))

#combine all HE
curr_he_final_tmp <- bind_rows(filter(curr_he2_tmp, !is.na(avg_comp_rate)),
                               filter(curr_he3_tmp, !is.na(avg_comp_rate)),
                               filter(curr_he4_tmp, !is.na(avg_comp_rate)),
                               filter(curr_he5_tmp))

he_sample_pass_tmp <- curr_he_final_tmp %>%
  group_by(curr_loe_code, curr_foe_code, age_group, state_code, gender, citizenship) %>%
  group_modify(~ HE_sample_f(.x)) %>%
  ungroup() %>%
  select(-c(avg_comp_rate, state_code, age_group)) %>%
  # add completion flag as pass for sampled
  mutate(completion_flag = "P")

he_sample_fail_tmp <- curr_he_final_tmp %>% 
  anti_join(he_sample_pass_tmp, by=("id" ="id")) %>%
  select(-c(avg_comp_rate, state_code, age_group)) %>%
  # add completion flag as fail 
  mutate(completion_flag = "F")

########################################## combine ##########################################
sample_all_tmp <- bind_rows(vet_sample_pass_tmp, he_sample_pass_tmp, vet_sample_fail_tmp, he_sample_fail_tmp) %>%
  select(-curr_foe2_code)

#join back to rest of population
passfailtertiary_df <- df %>%
  select(-age_group) %>%
  filter(!(id %in% sample_all_tmp$id)) %>%
  bind_rows(sample_all_tmp)

  return(passfailtertiary_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#create study countdown for new students

studyduration_fun <- function(df) {
  assigncd_tmp <- df %>%
  filter(stup == "studying", is.na(curr_countdown)) %>%
  mutate(curr_foe2_code = substr(curr_foe_code,1,2))

# assign course duration
set.seed(123)

prob_tmp <- assigncd_tmp %>%
  group_by(curr_loe_code, curr_foe2_code) %>%
  mutate(uni_prob = runif(n())) %>%
  ungroup()

duration_tmp <- prob_tmp %>% 
  left_join(maxcomp_df, join_by(curr_loe_code == loe, 
                                curr_foe2_code == foe2_code,
                                uni_prob >= prev_cumulative_prop,
                                uni_prob <= cumulative_prop)) %>%
  select(-c(uni_prob,cumulative_prop, prev_cumulative_prop))

#for passing students - take the max study duration
passdur_tmp <- duration_tmp %>%
  filter(completion_flag == "P") %>%
  mutate(curr_countdown = max_duration) %>%
  select(-c(max_duration, curr_foe2_code))

#for failing students - poisson distribution on the study duration
faildur_tmp <- duration_tmp %>%
  filter(completion_flag == "F") %>%
  group_by(curr_loe_code, curr_foe2_code, max_duration) %>%
  mutate(curr_countdown = assign_skewed_fail(n(), max_duration))%>%
  ungroup() %>%
  select(-c(max_duration, curr_foe2_code))

########################################## combine ##########################################
studydur_tmp <- bind_rows(passdur_tmp, faildur_tmp)

#join back to rest of population
studyduration_df <- df %>%
  filter(!(id %in% studydur_tmp$id)) %>%
  bind_rows(studydur_tmp)

  return(studyduration_df)
  rm(list = ls(pattern = "tmp"))
}

# COMMAND ----------

#function to update learning journey

learningjourney_fun <- function(df){
learningjourney_df <- df %>%
  mutate(learning_journey = ifelse(stup == "not_studying", NA,
                            ifelse(stup == "studying" & !is.na(learning_journey), learning_journey, "subsequent")))
return(learningjourney_df)
}