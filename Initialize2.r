
seq_combination_3w_read=read.csv("C:/Users/Shikar S/Desktop/Keyword Analysis/output_final.csv",as.is=T)


seq_combination_3w_read=seq_combination_3w_read[!duplicated(seq_combination_3w_read), ]
table(seq_combination_3w_read$total_occ)


check=seq_combination_3w_read[which(seq_combination_3w_read$conv_rate<90 & seq_combination_3w_read$conv_rate>50),]
head(check[order(check$total_occ,decreasing=TRUE),],n=20)
