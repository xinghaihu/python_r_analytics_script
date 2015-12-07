# R script for launching H2O, training Logistic regression models for YCP optimization

# author : @xinghai

helpFunc <- function() {
    cat("
          The R Script arguments:
          --h2oIP=10.216.53.24\                                                                                 - character
          --h2oPort=54321                                                                                       - numeric
          --datapath=hdfs://phazontan-nn1.tan.ygrid.yahoo.com:8020/user/gdongrid/xinghai/sampledTrainingData1   - character
          --log=20151117_151003.log                                                                             - character
          --h2oRFilePath=/homes/xinghai/h2o-3.2.0.9-hdp2.2/R/h2o_3.2.0.9.tar.gz                                 - character
          --productSubcatListFile=/homes/xinghai/psubcatList.txt                                                - character
          --out=/homes/xinghai/result.json                                                                      - character
          --productSubcatListFile=/homes/xinghai/psubcatList.txt                                                - character
          --help                                                                                                - print this text

          example:
          /homes/xinghai/R-3.2.2/bin/Rscript logisticRegression.R --h2oIP=10.216.53.24 --h2oPort=54321 --datapath=hdfs://phazontan-nn1.tan.ygrid.yahoo.com:8020/user/gdongrid/xinghai/sampledTrainingData1 --out=/homes/xinghai/result.json --log=20151117151003.log --h2oRFilePath=/homes/xinghai/h2o-3.2.0.9-hdp2.2/R/h2o_3.2.0.9.tar.gz --productSubcatListFile=/homes/xinghai/psubcatList.txt \n")
}

args <- commandArgs(trailingOnly = TRUE)

if(length(args) < 1) {
  args <- c("--help")
}

if("--help" %in% args) {
  helpFunc()
  q(save="no")
}

parseargs <- function(x) strsplit(sub("^--", "", x), "=")
argsdf <- as.data.frame(do.call("rbind", parseargs(args)))
argsl <- as.list(as.character(argsdf$V2))
names(argsl) <- argsdf$V1

if( "log" %in% names(argsl) ) {
  log_file = toString(argsl[ "log" ])
} else {
  log_file = paste(paste(strftime(Sys.time(), "%Y%m%d_%H%M%S"), ".R.log", sep=""))
}

cat( paste("Find log in:", log_file, "\n") )
sink(log_file)

if( "h2oIP" %in% names(argsl) ) {
  h2o_ip = toString(argsl[ "h2oIP" ])
} else {
  helpFunc()
  stop("h2o IP is missing.")
}
cat( paste("h2o ip:", h2o_ip) )
cat("\n")

if( "h2oPort" %in% names(argsl) ) {
  h2o_port = as.numeric(argsl[ "h2oPort" ])
} else {
  h2o_port = 54321
}
cat( paste("h2o port:", h2o_port) )
cat("\n")

if( "datapath" %in% names(argsl) ) {
  training_data_path = toString(argsl[ "datapath" ])
} else {
  helpFunc()
  stop("Import data path missing")
}
cat( paste("data path:", training_data_path) )
cat("\n")

if( "h2oRFilePath" %in% names(argsl) ) {
  h2o_r_file_path = toString(argsl["h2oRFilePath"])
} else {
  if (! ("h2o" %in% rownames(installed.packages())) ) {
    helpFunc()
    stop("H2O (R) install package missing.")
  }
}
cat( paste("R file path:", h2o_r_file_path) )
cat("\n")

if( "productSubcatListFile" %in% names(argsl) ) {
  productSubcatListFile = toString(argsl["productSubcatListFile"])
} else {
  helpFunc()
  stop("product subcateory list file is missing.")
}
cat( paste("product subcategory list file:", productSubcatListFile) )
cat("\n")

if( "out" %in% names(argsl) ) {
  outputFile = toString(argsl["out"])
} else {
  outputFile = "/homes/xinghai/result.json"
}
cat( paste("output file:", outputFile) )
cat("\n")

minimumEntryNumRequired = 60

install_related_packages <- function() {
    if (! ("methods" %in% rownames(installed.packages()))) { install.packages("methods", repos="http://cran.rstudio.com/") }
    if (! ("statmod" %in% rownames(installed.packages()))) { install.packages("statmod", repos="http://cran.rstudio.com/") }
    if (! ("stats" %in% rownames(installed.packages()))) { install.packages("stats", repos="http://cran.rstudio.com/") }
    if (! ("graphics" %in% rownames(installed.packages()))) { install.packages("graphics", repos="http://cran.rstudio.com/") }
    if (! ("RCurl" %in% rownames(installed.packages()))) { install.packages("RCurl", repos="http://cran.rstudio.com/") }
    if (! ("jsonlite" %in% rownames(installed.packages()))) { install.packages("jsonlite", repos="http://cran.rstudio.com/") }
    if (! ("tools" %in% rownames(installed.packages()))) { install.packages("tools", repos="http://cran.rstudio.com/") }
    if (! ("utils" %in% rownames(installed.packages()))) { install.packages("utils", repos="http://cran.rstudio.com/") }
    if (! ("rjson" %in% rownames(installed.packages()))) { install.packages("rjson", repos="http://cran.rstudio.com/") }
    if (! ("h2o" %in% rownames(installed.packages())) ) {install.packages(h2o_r_file_path, repos = NULL, type = "source")}
    cat("Packages install successfully.\n")
}

install_related_packages()

library(h2o)
library(rjson)
.h2o = h2o.init(ip = h2o_ip, port = h2o_port, startH2O = TRUE)
data.hex <- h2o.importFile(.h2o, path = training_data_path)
cat("Successfully initialize H2O and import data file.\n")
name <- c('pname', 'pcat', 'psubcat', 'agectr', 'genctr', 'geoctr', 'clk')
colnames(data.hex) <- name
X <- c('agectr', 'genctr', 'geoctr')
Y <- 'clk'

psubcatList <- as.list( read.csv(file = productSubcatListFile, header = FALSE) )
psubcatList <- psubcatList[[1]]
result <- vector(mode = "list", length = length(psubcatList) )
names(result) <- psubcatList

idx = 1
for (psubcatName in psubcatList) {
    dataFilter <- data.hex[data.hex$psubcat == psubcatName, ]
    entryNum <- nrow(dataFilter)
    if (entryNum < minimumEntryNumRequired)
    {
        result[[idx]] <- list(coefficients = NULL, metrics = NULL)
        idx = idx + 1
        cat( paste(psubcatName, "does not have enough data entry.\n") )
        next
    }
    validationSet_validation_num <- entryNum / 4
    trainset_validation_num <- entryNum - validationSet_validation_num
    trainset_validation <- dataFilter[1:trainset_validation_num, ]
    validationset_validation <- dataFilter[(trainset_validation_num+1):entryNum , ]

    # run on validation set to get model performance
    .h2o.glm <- h2o.glm(training_frame=trainset_validation, x=X, y=Y,  family = "binomial", alpha = 0.5, standardize=TRUE, lambda_search=TRUE)
    prediction <- h2o.predict(.h2o.glm, validationset_validation)
    performance <- h2o.performance(.h2o.glm, validationset_validation)
    summary(performance)
    auc <- h2o.auc(performance)
    if (auc < 0.1 ) {
        cat( paste(psubcatName, "fails to have good training performance.\n") )
        result[[idx]] <- list(coefficients = NULL, metrics = NULL)
        idx = idx + 1
        next
    }

    f1ScoreList <- h2o.F1(performance)
    f1Score <- max(f1ScoreList, na.rm=TRUE)
    maxF1Idx <- which(f1ScoreList == f1Score)
    accuracyList <- h2o.accuracy(performance)
    precisionList <- h2o.precision(performance)
    recallList <- h2o.recall(performance)
    accuracy <- as.numeric(accuracyList[maxF1Idx])
    precision <- as.numeric(precisionList[maxF1Idx])
    recall <- as.numeric(recallList[maxF1Idx])

    # run the model
    .h2o.glm.fin <- h2o.glm(training_frame=dataFilter, x=X, y=Y,  family = "binomial", alpha = 0.5, standardize=TRUE, lambda_search=TRUE)
    .h2o.glm.coef <- h2o.coef(.h2o.glm.fin)
    metrics <- list(auc = auc, accuracy = accuracy, precision = precision, recall = recall, f1 = f1Score)
    result[[idx]] <- list(coefficients = .h2o.glm.coef, metrics = metrics)
    print( paste( "Result for", psubcatName, "is", toString(result[[idx]]) ) )
    cat( paste(psubcatName, "done.\n") )
    idx = idx + 1
}

json = toJSON(result)
fileConnector = file(outputFile)
writeLines(json, fileConnector)

h2o.shutdown(.h2o, prompt = FALSE)
cat("H2O shut down.\n")
sink()
q(save="no")