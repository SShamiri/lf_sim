
grp_id = "2022NSWfemale25-34 years"

grp_flow <- function(pop_df, state_flow, state_pop, grp_id){
 
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
  
  return(grp_df)
  
}

ids <- unique(state_pop$grp_id)
out_lst = list()

for(j in 1:length(ids)){
  print(paste0('iter: ', j, ' id: ', ids[j]))
  
  out_lst[[j]] <- grp_flow(pop_df, state_flow, state_pop, ids[j])
}


grp_flow(pop_df, state_flow, state_pop, ids[1])
