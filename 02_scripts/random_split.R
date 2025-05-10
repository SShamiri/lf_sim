# Load necessary library
library(tidyverse)

##### CASE 1
# Create the first data frame (individuals)
individuals <- data.frame(
  id = c(paste0("A-", 1:9), paste0("B-", 1:7)),
  grp = c(rep("A", 9), rep("B", 7))
)

# Create the second data frame (destination counts)
dest_counts <- data.frame(
  grp = c("A", "A", "A", "B", "B", "B"),
  dest = c("emp", "unemp", "nilf", "emp", "unemp", "nilf"),
  n = c(3, 4, 2, 4, 2, 1)
)
# Notes:
# A data frame is randomly split into n datasets
# the the total rows of new created datasets HAVE to equal to the rows of the original datasets
## Note here grp A is spilt into 3 datasets: set 1 has 3 rows, set 2 has 4 rows and set 3 has 2 rows
# total of 9 rows, and the original dataset of group A has total of 9 rows
#

result <- individuals %>%
  group_by(grp) %>%
  group_modify(~ {
    # Get destination counts for this group
    dests <- dest_counts %>% 
      filter(grp == .y$grp) %>%
      uncount(n) %>%
      pull(dest)
    
    # Randomly assign
    set.seed(123)
    .x %>%
      mutate(dest = sample(dests))
  }) %>%
  ungroup()

result

### Data.Table Approach (Most Efficient)
library(data.table)

# Convert to data.table
setDT(individuals)
setDT(dest_counts)

# Efficient assignment using data.table
result <- individuals[, {
  # Get destinations for current group
  group_dests <- dest_counts[grp == .BY$grp]
  dest_vec <- rep(group_dests$dest, group_dests$n)
  
  # Random sample without replacement
  set.seed(123) # for reproducibility
  .(id = id, dest = sample(dest_vec))
}, by = grp]

# Verify counts
result[, .N, by = .(grp, dest)]

### Parallel Processing Version
# For extremely large datasets (10M+ rows):

library(parallel)
library(data.table)

# Convert to data.table
setDT(individuals)
setDT(dest_counts)

# Set up parallel processing
num_cores <- detectCores() - 1
cl <- makeCluster(num_cores)

# Split data by group for parallel processing
group_list <- split(individuals, f = "grp")

# Parallel assignment function
par_assign <- function(group_df, dest_counts) {
  library(data.table)
  grp <- group_df$grp[1]
  group_dests <- dest_counts[grp == dest_counts$grp]
  dest_vec <- rep(group_dests$dest, group_dests$n)
  group_df[, dest := sample(dest_vec)]
  return(group_df)
}

# Apply in parallel
clusterExport(cl, c("dest_counts"))
result_list <- parLapply(cl, group_list, par_assign, dest_counts)
stopCluster(cl)

# Combine results
result <- rbindlist(result_list)
result

### Memory-Efficient dplyr Approach
library(dplyr)
library(purrr)

# Split-apply-combine with map_df
result <- individuals %>%
  group_by(grp) %>%
  group_split() %>%
  map_df(~ {
    group_dests <- filter(dest_counts, grp == .x$grp[1])
    dest_vec <- rep(group_dests$dest, group_dests$n)
    .x %>%
      mutate(dest = sample(dest_vec))
  })


### Handling Very Large Data
# For datasets that don't fit in memory:
library(disk.frame)

# Set up disk.frame
setup_disk.frame()
options(future.globals.maxSize = Inf)

# Convert to disk.frame
individuals_df <- as.disk.frame(individuals)

# Process by group
result <- individuals_df %>%
  group_by(grp) %>%
  mutate(dest = {
    group_dests <- dest_counts[dest_counts$grp == .BY$grp,]
    dest_vec <- rep(group_dests$dest, group_dests$n)
    sample(dest_vec)
  }) %>%
  collect()

#### CASE 2
# Notes:
# A data frame is randomly split into n datasets
# the the total rows of new created datasets DON'T have to equal to the rows of the original datasets

# Your input data
vpop <- data.frame(
  id = c(paste0("A-", 1:9), paste0("B-", 1:7)),
  grp = c(rep("A", 9), rep("B", 7))
)

## Note here grp A is spilt into 3 datasets: set 1 has 3 rows, set 2 has 3 rows and set 3 has 2 rows
# total of 8 rows, whereas the original dataset of group A has total of 9 rows
#
counts <- data.frame(
  grp = c("A", "A", "A", "B", "B", "B"),
  dest = c("emp", "unemp", "nilf", "emp", "unemp", "nilf"),
  n = c(3, 3, 2, 3, 2, 1) 
)

library(dplyr)
library(tidyr)
library(purrr)

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

# View result
print(result)

# Function to assign individuals randomly by group
set.seed(123)  # for reproducibility
assigned <- do.call(rbind, lapply(split(counts, counts$grp), function(grp_counts) {
  # Get individuals in this group
  ids <- vpop$id[vpop$grp == grp_counts$grp[1]]
  # Shuffle
  ids <- sample(ids)
  # Assign based on counts
  splits <- rep(grp_counts$dest, grp_counts$n)
  data.frame(id = ids[1:length(splits)], grp = grp_counts$grp[1], dest = splits)
}))

## Efficient R Code Using data.table
library(data.table)

# Create data.tables
vpop <- data.table(
  id = c(paste0("A-", 1:9), paste0("B-", 1:7)),
  grp = c(rep("A", 9), rep("B", 7))
)

counts <- data.table(
  grp = c("A", "A", "A", "B", "B", "B"),
  dest = c("emp", "unemp", "nilf", "emp", "unemp", "nilf"),
  n = c(3, 3, 2, 3, 2, 1)
)

# Set seed for reproducibility
set.seed(123)

# Function to assign individuals based on counts
assign_groups <- function(pop_dt, count_dt) {
  result_list <- list()
  
  for (grp_val in unique(count_dt$grp)) {
    # Get individuals for this group
    ids <- pop_dt[grp == grp_val, id]
    
    # Shuffle the IDs
    ids <- sample(ids)
    
    # Get destinations and number of individuals to assign
    grp_counts <- count_dt[grp == grp_val]
    dests <- rep(grp_counts$dest, grp_counts$n)
    
    # Create assignment table
    result <- data.table(
      id = ids[seq_along(dests)],
      grp = grp_val,
      dest = dests
    )
    
    result_list[[grp_val]] <- result
  }
  
  rbindlist(result_list)
}

# Run the assignment
assigned <- assign_groups(vpop, counts)

# View result
print(assigned)
