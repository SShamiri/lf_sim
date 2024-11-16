
# Departure function ----
departures_fn <- function(pop, dep, dest_name = NULL){
  sample_sizes <- dep
  # Check that the sum of sample_sizes equals the number of rows
 # stopifnot(sum(sample_sizes) > nrow(pop))
  # Step 1: Randomly shuffle the row indices
  shuffled_indices <- sample(nrow(pop))
  
  # Step 2: Split the data into n samples based on sample sizes
  # Use the `split` and `cumsum` functions to split based on cumulative sizes
  if(!is.null(dest_name)) lab_names = dest_name
  
  split_indices <- split(shuffled_indices, 
                         cut(seq_along(shuffled_indices), 
                             breaks = cumsum(c(0, sample_sizes)), 
                             labels = lab_names))
  
  # Step 3: Extract each sample based on the indices
  sample_list <- lapply(split_indices, function(indices) pop[indices, ])
  
  # View the result
  sample_list |> bind_rows(.id = 'dest')
  
}