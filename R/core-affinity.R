#' Assign core number based on grouping vars
#'
#' Balance core load while maintaining groups together
#'
#' @param data Dataset
#' @param grouping_vars Character vector of grouping variables
#' @param n_cores Number of cores to divide the data among. Default parallel::detectCores()
#' @importFrom dplyr select group_by_at right_join vars one_of tally ungroup arrange desc
#' @importFrom tibble add_column
#' @importFrom magrittr %>%
#' @param
#' @export
#' @examples
#' mtcars %>%
#'   get_core_affinity(c('cyl', 'am')) %>%
#'   partition(core_n) %>%
#'   group_by(cyl, am) %>%
#'   arrange(mpg) %>%
#'   distinct(gear) %>%
#'   collect() %>%
#'   ungroup() %>%
#'   select(-core_n)
get_core_affinity = function(data, grouping_vars, n_cores = parallel::detectCores()) {
  data %>%
    group_by_at(vars(one_of(grouping_vars))) %>%
    tally() %>%
    ungroup() %>%
    arrange(desc(n)) %>%
    add_column(core_n = rep(1:n_cores, length.out = nrow(.))) %>%
    select(-n) %>%
    right_join(data)
}
