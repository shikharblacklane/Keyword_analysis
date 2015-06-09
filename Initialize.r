# Drop table if exists bi.keyword_data;
# create table bi.keyword_data (visit_time datetime,sid varchar(225),eid varchar(225),keyword varchar(225),conv int(11) );

# LOAD DATA LOCAL INFILE 'C:/Users/Shikar S/Desktop/Keyword Analysis/keywords.csv' INTO TABLE bi.transaction_data_raw fields terminated by ";" optionally enclosed by '"'  LINES TERMINATED BY '\r\n' IGNORE 1 LINES;



##########################################      R CODE      #########################################




#Run in R 32
# load the package
library(RODBC)
 
# connect to you new data source
db <- odbcConnect("exports", uid="exports", pwd="NmdX6yhBnLh82xEQ")
 
# view the names of the available tables
sqlTables(db)
 

#install.packages("tm")

library(tm)
df_full <- sqlQuery(db, "select sid,keyword from bi.keyword_data group by 1 ORDER BY RAND()")
df=df_full[1:(nrow(df_full)*0.6),]
df2=df_full[(nrow(df_full)*0.6+1):nrow(df_full),]

df$keyword=tolower(df$keyword)
##trimmer
trim <- function (x) gsub("^\\s+|\\s+$", "", x)
df$keyword=trim(df$keyword)

#Replace hyphen with space
df=df[setdiff(grep("",df$keyword),grep("://www.",df$keyword)),]
df=df[setdiff(grep("",df$keyword),grep("https:",df$keyword)),]
df=df[setdiff(grep("",df$keyword),grep("http:",df$keyword)),]



df$keyword=gsub("-", " ", df$keyword)

df$conversion =  sample(0:1,sum(df$sid!=0) ,replace=T)

corpus <- Corpus(VectorSource(df$keyword))

test <- tm_map(corpus, removeWords, c(stopwords("english"), stopwords("german")))

test <- tm_map(test, stripWhitespace)



##City names
dist_names <- sqlQuery(db, "select distinct name from business_districts;")
dist_names=tolower(dist_names$name)
dist_names=as.character(dist_names)

dist_names[grep("-",dist_names)]

dist_2names_div=unlist(strsplit(dist_names[grep("-",dist_names)],"-"))
dist_2names_div=dist_2names_div[nchar(dist_2names_div)>3]
dist_2names_div=setdiff(dist_2names_div,"city")

dist_names_all=c(dist_names,dist_2names_div)

	
################  Misspelling correction function   ################
##Normal words
# sorted_words <- names(sort(table(strsplit(tolower(paste(readLines("http://www.norvig.com/big.txt"), collapse = " ")), "[^a-z]+")), decreasing = TRUE))

# sorted_words=c(sorted_words,dist_names_all)

# correct <- function(word) { c(sorted_words[ adist(word, sorted_words) <= min(adist(word, sorted_words), 2)], word)[1] }



####################################################################




###Convert back to data.frame
dataframe<-data.frame(text=unlist(sapply(test, `[`, "content")), 
    stringsAsFactors=F)

	
df=cbind(df,dataframe)


write.csv(df,"Z:/R-Temp/df.csv",row.names=F)


names(df)[4]<-paste("filtered")



List <- strsplit(df$filtered, " ")
df_new=data.frame(Id=rep(df$sid, sapply(List, length)), Words=unlist(List))
df_new$Words=as.character(df_new$Words)
names(df_new)[1]<-"sid"


#df_new$Words_Corrected

# test=as.data.frame(sapply(df_new$Words,function(x) correct(x)))

df_new=merge(df,df_new,by="sid")


df_new_words=df_new[,c("Words","conversion")]



#install.packages("dplyr")










###Conversion Rate


data = df_new_words
data$buffer <- 1
data2=aggregate(data$conversion, list(words=data$Words), sum)
names(data2)[2]<-"total_conv"

data3=aggregate(data$buffer, list(words=data$Words), sum)
names(data3)[2]<-"total_occ"

data_final=merge(data2,data3,by="words")

data_final$conv_rate=data_final$total_conv/data_final$total_occ*100

data_final = data_final[-1:-9,]

data_final_pruned=(data_final[data_final$total_occ>3,])

data_final_pruned = data_final_pruned[-1,]


####TO REVIEW THIS CONDITION WITH FABIEN


data_final_filtered=(data_final_pruned[data_final_pruned$conv_rate>25  ,])  #maximum total_occ for conv_rate < 25 is 13

table(as.data.frame(table(df_new$sid))[2])

names(data_final_filtered)[1]<-paste("Words")

df_new_filtered=merge(df_new, data_final_filtered, by="Words")


df_new_filtered <- df_new_filtered[with(df_new_filtered, order(sid)), ]

###Combine only two words filtered+unfiltered -> approx 150 x 1700 x 2 number of possibilities. Conv rate would be of no use. (new matches with combinations would have to be grouped)





# x <- c("I want to break free", "I am alive", "Because of you I am awake", "Arriving somewIhere buam not here", "Where am I living now","India china am","China america","America india","I am" )
# pattern <- "(^|\\s)I(\\s.*)?\\sam($|\\s)"
# grep(pattern, x)

filtered_keys=data_final_filtered$Words
all_keys=data_final$words
combination_2w_1=as.data.frame(expand.grid(filtered_keys,all_keys))
combination_2w_2=as.data.frame(expand.grid(all_keys,filtered_keys))

seq_combination_2w=rbind(combination_2w_1,combination_2w_2)



#### Create a for loop on the following condition with df_new data and 
seq_combination_2w$pattern <-  paste(paste(paste("(^|\\s)",seq_combination_2w$Var1,sep=""),seq_combination_2w$Var2,sep="(\\s.*)?\\s"),"($|\\s)",sep="")

seq_combination_2w$seq_combination <-  paste(seq_combination_2w$Var1,seq_combination_2w$Var2,sep=" ")
write.csv(seq_combination_2w,"Z:/R-Temp/to_run.csv",row.names=F)

seq_combination_2w=read.csv("Z:/R-Temp/to_run.csv",as.is=T)

#comb_test=seq_combination_2w[which(seq_combination_2w$Var1=="s"),]


# for(i in 1:nrow(comb_test))
# {
# comb_test[i,"total_occ"]= length(grep(comb_test[i,"pattern"], df$filtered))
# comb_test[i,"total_conv"]= length(which(df[grep(comb_test[i,"pattern"], df$filtered),"conversion"]==1))
# }


for(i in 1:nrow(seq_combination_2w))
{
seq_combination_2w[i,"total_occ"]= length(grep(seq_combination_2w[i,"pattern"], df$filtered))
seq_combination_2w[i,"total_conv"]= length(which(df[grep(seq_combination_2w[i,"pattern"], df$filtered),"conversion"]==1))
}

seq_combination_2w_read$conv_rate=seq_combination_2w_read$total_conv/seq_combination_2w_read$total_occ*100

write.csv(seq_combination_2w_read,"C:/Users/Shikar S/Desktop/Keyword Analysis/output.csv",row.names=F)

seq_combination_2w_read=read.csv("C:/Users/Shikar S/Desktop/Keyword Analysis/output.csv",as.is=T)


seq_combination_2w_read=seq_combination_2w_read[!duplicated(seq_combination_2w_read), ]
table(seq_combination_2w_read$total_occ)

###Association Combination 
combination_2w_11= seq_combination_2w_read[seq_combination_2w_read$Var1 %in% filtered_keys,]

combination_2w_22= seq_combination_2w_read[seq_combination_2w_read$Var2 %in% filtered_keys,]
names(combination_2w_22)[5] <- paste("total_occ2")
names(combination_2w_22)[6] <- paste("total_conv2")

asso_combination=merge(combination_2w_11,combination_2w_22,by.x=c("Var1","Var2"),by.y=c("Var2","Var1"))
names(asso_combination)
# [1] "Var1"              "Var2"              "pattern.x"         "seq_combination.x" "total_occ"         "total_conv"        "conv_rate.x"       "pattern.y"         "seq_combination.y"
# [10] "total_occ2"        "total_conv2"       "conv_rate.y" 
asso_combination$pattern.x <-NULL
asso_combination$pattern.y <-NULL
asso_combination$seq_combination.x <-NULL
asso_combination$seq_combination.y <-NULL
asso_combination$conv_rate.x <-NULL
asso_combination$conv_rate.y <-NULL

asso_combination$occ <- asso_combination$total_occ + asso_combination$total_occ2

asso_combination$conv <- asso_combination$total_conv + asso_combination$total_conv2

asso_combination$total_occ <-NULL
asso_combination$total_occ2 <-NULL
asso_combination$total_conv <-NULL
asso_combination$total_conv2 <-NULL

asso_combination$conv_rate=asso_combination$conv/asso_combination$occ*100
asso_combination[which(asso_combination$Var1==asso_combination$Var2),"occ"]= asso_combination[which(asso_combination$Var1==asso_combination$Var2),"occ"]/2

asso_combination[which(asso_combination$Var1==asso_combination$Var2),"conv"]= asso_combination[which(asso_combination$Var1==asso_combination$Var2),"conv"]/2

write.csv(asso_combination,"C:/Users/Shikar S/Desktop/Keyword Analysis/asso_combination.csv",row.names=F)


#### Define Conv Rate, calculate support, calculate confidence. Set threshold on confidence and conv rate. 


check=seq_combination_2w_read[which(seq_combination_2w_read$conv_rate<90 & seq_combination_2w_read$conv_rate>50),]
head(check[order(check$total_occ,decreasing=TRUE),],n=20)

summary(seq_combination_2w_read)
     # Var1               Var2             pattern          seq_combination      total_occ           total_conv         conv_rate     
 # Length:309192      Length:309192      Length:309192      Length:309192      Min.   :  0.00000   Min.   : 0.00000   Min.   :  0.00  
 # Class :character   Class :character   Class :character   Class :character   1st Qu.:  0.00000   1st Qu.: 0.00000   1st Qu.:  0.00  
 # Mode  :character   Mode  :character   Mode  :character   Mode  :character   Median :  0.00000   Median : 0.00000   Median : 50.00  
 #                                                                           # Mean   :  0.04796   Mean   : 0.02356   Mean   : 48.58  
#                                                                            # 3rd Qu.:  0.00000   3rd Qu.: 0.00000   3rd Qu.:100.00  
#                                                                            # Max.   :164.00000   Max.   :83.00000   Max.   :100.00  
#

df[grep("(^|\\s)orlando(\\s.*)?\\sorlando($|\\s)", df$filtered),]


head(seq_combination_2w_read[which(seq_combination_2w_read$total_occ>33),],n=50)


seq_combination_2w_read= seq_combination_2w_read[which(seq_combination_2w_read$total_occ>0),]
table(seq_combination_2w_read$total_occ)




###Filter on min. num of total occurrences of 10 and min conv rate or 25   



seq_combination_filtered= seq_combination_2w_read[which(seq_combination_2w_read$total_occ>4),c("Var1","Var2")]

seq_combination_filtered[,"Var3"]<- "999"

a=seq_combination_filtered

for(i in 1:length(dist_names_all))
{
a$Var3<- dist_names_all[i]
seq_combination_filtered=rbind(seq_combination_filtered,a)
}

seq_combination_filtered=seq_combination_filtered[which(seq_combination_filtered$Var3!=999),]

seq_combination_filtered2=seq_combination_filtered
names(seq_combination_filtered2)<-paste("a",names(seq_combination_filtered2),sep="_")
names(seq_combination_filtered2)[1]<-paste("Var1")
names(seq_combination_filtered2)[3]<-paste("Var2")
names(seq_combination_filtered2)[2]<-paste("Var3")



seq_combination_filtered3=seq_combination_filtered
names(seq_combination_filtered3)<-paste("a",names(seq_combination_filtered3),sep="_")
names(seq_combination_filtered3)[1]<-paste("Var2")
names(seq_combination_filtered3)[3]<-paste("Var1")
names(seq_combination_filtered3)[2]<-paste("Var3")


seq_with_district=rbind(seq_combination_filtered,seq_combination_filtered2,seq_combination_filtered3)


# x <- c("I want to break free", "I Berlinam alive", "Because of you I am awake", "Arriving I aaa Berlin am somewIhere buam not here", "I Berlin Where am I living now","India Berlin china am","China america","America india","I am" )
# pattern <- "(^|\\s)I(\\s.*)?\\sBerlin(\\s.*)?\\sam($|\\s)"
# grep(pattern, x)



seq_with_district$pattern <-  paste(paste(paste("(^|\\s)",seq_with_district$Var1,sep=""),seq_with_district$Var2,seq_with_district$Var3,sep="(\\s.*)?\\s"),"($|\\s)",sep="")

seq_with_district$seq_combination <-  paste(seq_with_district$Var1,seq_with_district$Var2,seq_with_district$Var3,sep=" ")

write.csv(seq_with_district,"Z:/R-Temp/to_run2.csv",row.names=F)

seq_with_district=read.csv("C:/Users/Shikar S/Desktop/Keyword Analysis/to_run2.csv",as.is=T)

comb_test=seq_with_district[which(seq_with_district$Var1=="airport"),]


for(i in 1:nrow(comb_test))
{
comb_test[i,"total_occ"]= length(grep(comb_test[i,"pattern"], df$filtered))
comb_test[i,"total_conv"]= length(which(df[grep(comb_test[i,"pattern"], df$filtered),"conversion"]==1))
}


for(i in 1:nrow(seq_with_district))
{
seq_with_district[i,"total_occ"]= length(grep(seq_with_district[i,"pattern"], df$filtered))
seq_with_district[i,"total_conv"]= length(which(df[grep(seq_with_district[i,"pattern"], df$filtered),"conversion"]==1))
}


write.csv(seq_with_district,"C:/Users/Shikar S/Desktop/Keyword Analysis/output_final.csv",row.names=F)




