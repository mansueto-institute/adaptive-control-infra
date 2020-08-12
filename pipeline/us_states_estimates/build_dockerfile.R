if(!require(containerit)){
  devtools::install_github("o2r-project/containerit")
  library(containerit)
}

script <- system.file("schedulescripts", "cori_model.R", package="googleComputeEngineR")
file.copy(script, getwd())

## it will run the script whilst making the dockerfile
rmd_dockerfile <- containerit::dockerfile(from = "cori_model.R",
                                          image = "rocker/verse:3.6.1",
                                          soft = TRUE,
                                          cmd = CMD_Rscript("cori_model.R"),
                                          filter_baseimage_pkgs = TRUE)
write(rmd_dockerfile, file = "Dockerfile")
