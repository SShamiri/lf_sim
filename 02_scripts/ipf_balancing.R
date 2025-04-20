
#This is a classic problem known as matrix balancing or biproportional fitting.
#One common algorithm to achieve this is the Iterative Proportional Fitting (IPF) algorithm.

ipf_balancing <- function(dat, target_row_sums, target_col_sums, iter = 1000, tolerance = 1e-6){
  
  # Convert the data frame to a matrix for easier calculations
  matrix_data <- dat |> 
    column_to_rownames(var =colnames(dat)[1]) |>
    as.matrix()
  #matrix_data <- as.matrix(dat)
  # IPF Algorithm
  max_iterations <- iter
  tolerance <- tolerance
  current_matrix <- matrix_data
  for (iteration in 1:max_iterations) {
    # Adjust rows
    row_factors <- target_row_sums / rowSums(current_matrix)
    current_matrix <- sweep(current_matrix, 1, row_factors, "*")

    # Adjust columns
    col_factors <- target_col_sums / colSums(current_matrix)
    current_matrix <- sweep(current_matrix, 2, col_factors, "*")

    # Check for convergence
    if (all(abs(rowSums(current_matrix) - target_row_sums) < tolerance) &&
        all(abs(colSums(current_matrix) - target_col_sums) < tolerance)) {
      cat("Converged after", iteration, "iterations.\n")
      break
    }
    if (iteration == max_iterations) {
      cat("Maximum iterations reached. Convergence not fully achieved.\n")
      }
    }
  # Convert the adjusted matrix back to a data frame
  dat_adjusted <- as.data.frame(current_matrix) |>
    rownames_to_column(names(dat)[1]) |>
    as_tibble()
  # colnames(dat_adjusted) <- colnames(dat)
  # rownames(dat_adjusted) <- rownames(dat)

    return(dat_adjusted)
  #matrix_data
  #target_row_sums
  }

## Example
# Your initial data frame
# dat_init <- data.frame(
#   employed = c(203034, 8101, 3771),
#   unemployed = c(6492, 5763, 9543),
#   nilf = c(7934, 75619, 5886),
#   row.names = c("employed", "nilf", "unemployed")
# )
# 
# # Target row and column sums
# row_sums <- c(192252, 110779, 23113)
# col_sums <- c(214906, 21798, 89439)
# 
# ipf_balancing(dat_init, row_sums, col_sums)
# Verify the row and column sums
# print(rowSums(dat_adjusted))
# print(colSums(dat_adjusted))
