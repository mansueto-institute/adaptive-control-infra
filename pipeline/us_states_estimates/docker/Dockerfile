FROM r-base

EXPOSE 8080

RUN Rscript --no-save -e "print('hello world')"

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
