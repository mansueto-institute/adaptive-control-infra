FROM r-base
WORKDIR rscripts
COPY . .
RUN Rscript -e "install.packages(c('EpiEstim','tidyverse','lubridate'))"
CMD ["Rscript", "cori_model.R"]