library(tidyverse)
library(janitor)
library(tictoc)

dat <- read_csv("01_data/00_raw/SummarisedCoPFlowImplications.csv") |> 
  clean_names()

pop_df <- dat |> 
  filter(year == 2022, age >= 15) |> 
  #filter(year == 2022, age %in% c(28, 30), pop > 0, gccsa %in% c("SYD", "MELB")) |> 
  select(year, gccsa, sex, age, pop, nim_dep) |> 
  mutate(pop = pop + nim_dep) |> 
  uncount(pop) |>
  mutate(id = row_number()) |> 
  relocate(id) |> 
  group_by(year, gccsa, sex, age) |> 
  mutate(group_id = cur_group_id()) |> 
  ungroup() |> 
  arrange(group_id)

pop_df 

dep_df <- dat |> 
  filter(year == 2022, age >= 15) |> 
  #filter(year == 2022, age %in% c(28, 30), pop > 0, gccsa %in% c("SYD", "MELB")) |> 
  select(year, gccsa, sex, age, pop, 14:last_col()) |> 
  pivot_longer(!c(year:pop), names_to = "dest_gccsa", values_to = "dep") |> 
  group_by(year, gccsa, sex, age) |> 
  mutate(group_id = cur_group_id(),
         dest_gccsa = toupper(dest_gccsa)) |> 
  ungroup() |> 
  arrange(group_id)

dep_df


smp_pop <- pop_df |> filter(age == 38, sex == "M", gccsa == 'SYD')
smp_pop

smp_dep <- dep_df |> 
  filter(age == 38, sex == "M", gccsa == 'SYD') |> 
  mutate(dep = ifelse(gccsa == dest_gccsa, pop, dep)) |> 
  filter(dep > 0) |> 
  mutate( prop = dep/sum(dep))

smp_dep

ind <- sample(1:nrow(smp_dep), size = nrow(smp_pop), replace = T, prob = smp_dep$prop)

length(unique(ind))
length(smp_dep$prop)
length(smp_dep$dest_gccsa)

dest_names <- smp_dep |> filter(prop > 0) |> pull(dest_gccsa)

lst <- setNames(split(smp_pop,ind), smp_dep$dest_gccsa)

lst[2]

smp_pop |> 
  #sample_n(1153)
  sample_frac(2.651855e-02)

###############################

# Sample dataframe
set.seed(123)  # For reproducibility
data <- data.frame(ID = 1:100, Value = rnorm(100))  # 100 rows of data

# Define sample sizes (make sure they sum up to the total number of rows)
sample_sizes <- c(10, 20, 30, 25, 15)

# Check that the sum of sample_sizes equals the number of rows
stopifnot(sum(sample_sizes) == nrow(data))

# Step 1: Randomly shuffle the row indices
shuffled_indices <- sample(nrow(data))

# Step 2: Split the data into 5 samples based on sample sizes
# Use the `split` and `cumsum` functions to split based on cumulative sizes
split_indices <- split(shuffled_indices, 
                       cut(seq_along(shuffled_indices), 
                           breaks = cumsum(c(0, sample_sizes)), 
                           labels = FALSE))

# Step 3: Extract each sample based on the indices
sample_list <- lapply(split_indices, function(indices) data[indices, ])

# View the result
sample_list


