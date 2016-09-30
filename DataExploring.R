library(ggplot2)
library(dplyr)
library(gtable)
library(grid)
#----------------------------------------------------------------------------------------------
timebucket = function(x, bucketsize = 1,
                      units = c("secs", "mins",  "hours", "days", "weeks")) {
  secs = as.numeric(as.difftime(bucketsize, units=units[1]), units="secs")
  #structure(floor(as.numeric(x) / secs) * secs, class=c('POSIXt','POSIXct'))
  format(structure(floor(as.numeric(x) / secs) * secs, class=c('POSIXt','POSIXct')), "%H:%M")
}
#----------------------------------------------------------------------------------------------
timebucket_dt = function(x, bucketsize = 1,
                      units = c("secs", "mins",  "hours", "days", "weeks")) {
  secs = as.numeric(as.difftime(bucketsize, units=units[1]), units="secs")
  structure(floor(as.numeric(x) / secs) * secs, class=c('POSIXt','POSIXct'))
}
#----------------------------------------------------------------------------------------------
DoubleAxisChart = function(chart1,chart2) {
  #http://heareresearch.blogspot.com/2014/10/10-30-2014-dual-y-axis-graph-ggplot2_30.html
  grid.newpage()
  g1<-ggplot_gtable(ggplot_build(chart1))
  g2<-ggplot_gtable(ggplot_build(chart2))
  
  pp<-c(subset(g1$layout,name=="panel",se=t:r))
  g<-gtable_add_grob(g1, g2$grobs[[which(g2$layout$name=="panel")]],pp$t,pp$l,pp$b,pp$l)
  
  ia<-which(g2$layout$name=="axis-l")
  ga <- g2$grobs[[ia]]
  ax <- ga$children[[2]]
  ax$widths <- rev(ax$widths)
  ax$grobs <- rev(ax$grobs)
  ax$grobs[[1]]$x <- ax$grobs[[1]]$x - unit(1, "npc") + unit(0.15, "cm")
  g <- gtable_add_cols(g, g2$widths[g2$layout[ia, ]$l], length(g$widths) - 1)
  g <- gtable_add_grob(g, ax, pp$t, length(g$widths) - 1, pp$b)
  
  grid.draw(g)
  
}
#----------------------------------------------------------------------------------------------
WeekDays = c("Monday", "Tuesday", "Wednesday", "Thursday", "Friday")
WeekEnds = c("Saturday", "Sunday")
Morning.Rush.Hours = c("06:00","07:00","08:00","09:00","10:00")
Evening.Rush.Hours = c("15:00","16:00","17:00","18:00","19:00")
Day.Hours = c("11:00","12:00","13:00","14:00")
Evening.Hours = c("20:00","21:00","22:00","23:00","00:00")
Night.Hours = c("01:00","02:00","03:00","04:00","05:00")
#----------------------------------------------------------------------------------------------
df_rtd <- read.csv('/Users/drogaieva/Documents/Python/Bart_scraper/real_time_data.csv')
str(df_rtd)

df_rtd$Day <- weekdays(as.Date(df_rtd$TimeMark))
df_rtd$TimeMark <- as.POSIXct(strptime(x = as.character(df_rtd$TimeMark),format = "%Y-%m-%d %H:%M:%S"))
df_rtd$ScheduledTime <- as.POSIXct(strptime(x = as.character(df_rtd$ScheduledTime),format = "%Y-%m-%d %H:%M:%S"))
df_rtd$Label <- ifelse(df_rtd$Delay > 0,"Delayed", "OnTime")
df_rtd$Hour <- timebucket(df_rtd$TimeMark, units="hours")
str(df_rtd)
head(df_rtd)
summary(df_rtd)
dim(df_rtd)
nrow(df_rtd)
ncol(df_rtd)
colnames(df_rtd)
#----------------------------------------------------------------------------------------------
AggData = function (days = c(WeekDays , WeekEnds)) {
  
#group real time departures by hour
#Delayed
df.dth <- data.frame(count(filter(df_rtd,Delay>0, Day %in% days), timebucket_dt(TimeMark, units="hours"), Station, Destination, Line))
colnames(df.dth) <- c("DateTime", "Station", "Destination", "Line", "Departure.Num")
df.dth["Label"] <- "Delayed"
#head(df.dth)

#OnTime
df.dthnd <- data.frame(count(filter(df_rtd,Delay==0, Day %in% days), timebucket_dt(TimeMark, units="hours"), Station, Destination, Line))
colnames(df.dthnd) <- c("DateTime", "Station", "Destination", "Line", "Departure.Num")
df.dthnd["Label"] <- "OnTime"
#head(df.dthnd)

return (rbind(df.dth, df.dthnd))

}
#----------------------------------------------------------------------------------------------
HourlyDepartures = function ( df.agg
                             ,days = c(WeekDays , WeekEnds)
                             ) 
{
  
#num departures
df.num.dep <- data.frame(count(df.agg, timebucket(DateTime, units="hours"), Label))
colnames(df.num.dep) <- c("Time", "Label", "Departure.Num")
#tail(df.num.dep)


#count observations
df.count.obs <- data.frame(count(distinct(select(df.agg,DateTime)), timebucket(DateTime, units="hours")))
colnames(df.count.obs) <- c("Time", "Count.Observations")
df.count.obs1 <- df.count.obs
df.count.obs1$Label <- "OnTime"
df.count.obs2 <- df.count.obs
df.count.obs2$Label <- "Delayed"
df.count.obs <- rbind(df.count.obs1,df.count.obs2)
#tail(df.count.obs)


df.avg.dep.num <- data.frame(merge(df.num.dep,df.count.obs), check.names = FALSE)
df.avg.dep.num <- df.avg.dep.num[, unique(colnames(df.avg.dep.num))]
df.avg.dep.num$Departure.Avg.Num <- df.avg.dep.num$Departure.Num/df.avg.dep.num$Count.Observation
#tail(df.avg.dep.num)

g.count_delays <- ggplot(df.avg.dep.num, aes(x=Time,y=Departure.Avg.Num))
g.count_delays <- g.count_delays +  geom_bar(stat="identity",aes(fill=Label))
g.count_delays <- g.count_delays + xlab("Time") + ylab("Avg Count")
g.count_delays <- g.count_delays + ggtitle("Hourly Departures/Delays") +
  theme_bw()+
  theme(legend.justification=c(0,1),
        legend.position=c(0,1),
        plot.title=element_text(size=30,vjust=1),
        axis.text.x=element_text(size=20,angle = 90, hjust = 1),
        axis.text.y=element_text(size=20),
        axis.title.x=element_text(size=20),
        axis.title.y=element_text(size=20)
  )
#g.count_delays

g.avg_delay <- ggplot(filter(df_rtd,Day %in% days), aes(x=timebucket(TimeMark, units="hours"),y=Delay, group = 1))
g.avg_delay <- g.avg_delay + stat_summary(fun.y="mean", geom="line",colour = "red")
g.avg_delay <- g.avg_delay + xlab("Time") + ylab("Average Delay")
g.avg_delay <- g.avg_delay + ggtitle("Hourly Delays")+
  theme_bw() %+replace% 
  theme(panel.background = element_rect(fill = NA),
        panel.grid.major.x=element_blank(),
        panel.grid.minor.x=element_blank(),
        panel.grid.major.y=element_blank(),
        panel.grid.minor.y=element_blank(),
        axis.text.y=element_text(size=20,color="red"),
        axis.title.y=element_text(size=20),
        axis.text.x = element_text(angle = 90, hjust = 1)
  )
#g.avg_delay

DoubleAxisChart(g.count_delays,g.avg_delay)

}

#----------------------------------------------------------------------------------------------
DeparturesByStation = function (df.agg
                               ,days = c(WeekDays , WeekEnds)
                               ,hours = c(Morning.Rush.Hours,
                                          Evening.Rush.Hours,
                                          Day.Hours,
                                          Evening.Hours,
                                          Night.Hours)
                                ,destinations = c("24TH","CONC","DALY","DUBL","FRMT","MLBR","NCON","PHIL","PITT","RICH","SFIA")
) 
{
  
  #num departures
  df.num.dep <- data.frame(count(filter(df.agg, Destination %in% destinations), timebucket(DateTime, units="hours"), Station, Label))
  colnames(df.num.dep) <- c("Time","Station", "Label", "Departure.Num")
  #head(df.num.dep)
  
  
  #count observations
  df.count.obs <- data.frame(count(distinct(select(df.agg,DateTime,Station)), timebucket(DateTime, units="hours"),Station))
  colnames(df.count.obs) <- c("Time", "Station", "Count.Observations")
  df.count.obs1 <- df.count.obs
  df.count.obs1$Label <- "OnTime"
  df.count.obs2 <- df.count.obs
  df.count.obs2$Label <- "Delayed"
  df.count.obs <- rbind(df.count.obs1,df.count.obs2)
  #head(df.count.obs)
  
  
  df.avg.dep.num <- data.frame(merge(df.num.dep,df.count.obs), check.names = FALSE)
  #tail(filter(df.avg.dep.num,Station=='12TH'))
  df.avg.dep.num <- data.frame(filter(df.avg.dep.num,Time %in% hours ) %>% group_by(Station,Label) %>% 
                                 summarise(Departure.Num = sum(Departure.Num),Count.Observations = max(Count.Observations))
  )
  #head(filter(df.avg.dep.num,Station=='12TH'))
  df.avg.dep.num$Departure.Avg.Num <- df.avg.dep.num$Departure.Num/df.avg.dep.num$Count.Observation
  
  
  g.count_delays <- ggplot(filter(df.avg.dep.num), aes(x=Station,y=Departure.Avg.Num))
  g.count_delays <- g.count_delays +  geom_bar(stat="identity",aes(fill=Label))
  g.count_delays <- g.count_delays + xlab("Station") + ylab("Avg Count")
  g.count_delays <- g.count_delays + ggtitle("Departures/Delays by Station") +
    theme_bw()+
    theme(legend.justification=c(0,1),
          legend.position=c(0,1),
          plot.title=element_text(size=30,vjust=1),
          axis.text.x=element_text(size=20,angle = 90, hjust = 1),
          axis.text.y=element_text(size=20),
          axis.title.x=element_text(size=20),
          axis.title.y=element_text(size=20)
    )
  #g.count_delays
  
  g.avg_delay <- ggplot(filter(df_rtd,Day %in% days, Hour %in% hours, Destination %in% destinations), aes(x=Station,y=Delay, group = 1))
  g.avg_delay <- g.avg_delay + stat_summary(fun.y="mean", geom="line",colour = "red")
  g.avg_delay <- g.avg_delay + xlab("Station") + ylab("Average Delay")
  g.avg_delay <- g.avg_delay + ggtitle("Delays by Station")+
    theme_bw() %+replace% 
    theme(panel.background = element_rect(fill = NA),
          panel.grid.major.x=element_blank(),
          panel.grid.minor.x=element_blank(),
          panel.grid.major.y=element_blank(),
          panel.grid.minor.y=element_blank(),
          axis.text.y=element_text(size=20,color="red"),
          axis.title.y=element_text(size=20),
          axis.text.x = element_text(angle = 90, hjust = 1)
    )
  #g.avg_delay
  
  DoubleAxisChart(g.count_delays,g.avg_delay)
  
}

#----------------------------------------------------------------------------------------------
DeparturesByDestination = function (df.agg
                                ,days = c(WeekDays , WeekEnds)
                                ,hours = c(Morning.Rush.Hours,
                                           Evening.Rush.Hours,
                                           Day.Hours,
                                           Evening.Hours,
                                           Night.Hours)
) 
{
  
  #num departures
  df.num.dep <- data.frame(count(df.agg, timebucket(DateTime, units="hours"), Destination, Label))
  colnames(df.num.dep) <- c("Time","Destination", "Label", "Departure.Num")
  #head(df.num.dep)
  
  
  #count observations
  df.count.obs <- data.frame(count(distinct(select(df.agg,DateTime,Destination)), timebucket(DateTime, units="hours"),Destination))
  colnames(df.count.obs) <- c("Time", "Destination", "Count.Observations")
  df.count.obs1 <- df.count.obs
  df.count.obs1$Label <- "OnTime"
  df.count.obs2 <- df.count.obs
  df.count.obs2$Label <- "Delayed"
  df.count.obs <- rbind(df.count.obs1,df.count.obs2)
  #head(df.count.obs)
  
  
  df.avg.dep.num <- data.frame(merge(df.num.dep,df.count.obs), check.names = FALSE)
  df.avg.dep.num <- data.frame(filter(df.avg.dep.num,Time %in% hours ) %>% group_by(Destination,Label) %>% 
                                 summarise(Departure.Num = sum(Departure.Num),Count.Observations = max(Count.Observations))
  )
  df.avg.dep.num$Departure.Avg.Num <- df.avg.dep.num$Departure.Num/df.avg.dep.num$Count.Observation
  
  
  g.count_delays <- ggplot(filter(df.avg.dep.num), aes(x=Destination,y=Departure.Avg.Num))
  g.count_delays <- g.count_delays +  geom_bar(stat="identity",aes(fill=Label))
  g.count_delays <- g.count_delays + xlab("Destination") + ylab("Avg Count")
  g.count_delays <- g.count_delays + ggtitle("Departures/Delays by Destination") +
    theme_bw()+
    theme(legend.justification=c(0,1),
          legend.position=c(0,1),
          plot.title=element_text(size=30,vjust=1),
          axis.text.x=element_text(size=20,angle = 90, hjust = 1),
          axis.text.y=element_text(size=20),
          axis.title.x=element_text(size=20),
          axis.title.y=element_text(size=20)
    )
  #g.count_delays
  
  g.avg_delay <- ggplot(filter(df_rtd,Day %in% days, Hour %in% hours), aes(x=Destination,y=Delay, group = 1))
  g.avg_delay <- g.avg_delay + stat_summary(fun.y="mean", geom="line",colour = "red")
  g.avg_delay <- g.avg_delay + xlab("Destination") + ylab("Average Delay")
  g.avg_delay <- g.avg_delay + ggtitle("Delays by Destination")+
    theme_bw() %+replace% 
    theme(panel.background = element_rect(fill = NA),
          panel.grid.major.x=element_blank(),
          panel.grid.minor.x=element_blank(),
          panel.grid.major.y=element_blank(),
          panel.grid.minor.y=element_blank(),
          axis.text.y=element_text(size=20,color="red"),
          axis.title.y=element_text(size=20),
          axis.text.x = element_text(angle = 90, hjust = 1)
    )
  #g.avg_delay
  
  DoubleAxisChart(g.count_delays,g.avg_delay)
  
}
#----------------------------------------------------------------------------------------------
DeparturesByLine = function (df.agg
                             ,days = c(WeekDays , WeekEnds)
                             ,hours = c(Morning.Rush.Hours,
                                        Evening.Rush.Hours,
                                        Day.Hours,
                                        Evening.Hours,
                                        Night.Hours)
) 
{
  
  #num departures
  df.num.dep <- data.frame(count(df.agg, timebucket(DateTime, units="hours"), Line, Label))
  colnames(df.num.dep) <- c("Time","Line", "Label", "Departure.Num")
  #head(df.num.dep)
  
  
  #count observations
  df.count.obs <- data.frame(count(distinct(select(df.agg,DateTime,Line)), timebucket(DateTime, units="hours"),Line))
  colnames(df.count.obs) <- c("Time", "Line", "Count.Observations")
  df.count.obs1 <- df.count.obs
  df.count.obs1$Label <- "OnTime"
  df.count.obs2 <- df.count.obs
  df.count.obs2$Label <- "Delayed"
  df.count.obs <- rbind(df.count.obs1,df.count.obs2)
  #head(df.count.obs)
  
  
  df.avg.dep.num <- data.frame(merge(df.num.dep,df.count.obs), check.names = FALSE)
  df.avg.dep.num <- data.frame(filter(df.avg.dep.num,Time %in% hours ) %>% group_by(Line,Label) %>% 
                                 summarise(Departure.Num = sum(Departure.Num),Count.Observations = max(Count.Observations))
  )
  df.avg.dep.num$Departure.Avg.Num <- df.avg.dep.num$Departure.Num/df.avg.dep.num$Count.Observation
  
  
  g.count_delays <- ggplot(filter(df.avg.dep.num), aes(x=Line,y=Departure.Avg.Num))
  g.count_delays <- g.count_delays +  geom_bar(stat="identity",aes(fill=Label))
  g.count_delays <- g.count_delays + xlab("Line") + ylab("Avg Count")
  g.count_delays <- g.count_delays + ggtitle("Departures/Delays by Line") +
    theme_bw()+
    theme(legend.justification=c(0,1),
          legend.position=c(0,1),
          plot.title=element_text(size=30,vjust=1),
          axis.text.x=element_text(size=20,angle = 90, hjust = 1),
          axis.text.y=element_text(size=20),
          axis.title.x=element_text(size=20),
          axis.title.y=element_text(size=20)
    )
  #g.count_delays
  
  g.avg_delay <- ggplot(filter(df_rtd,Day %in% days, Hour %in% hours), aes(x=Line,y=Delay, group = 1))
  g.avg_delay <- g.avg_delay + stat_summary(fun.y="mean", geom="line",colour = "red")
  g.avg_delay <- g.avg_delay + xlab("Line") + ylab("Average Delay")
  g.avg_delay <- g.avg_delay + ggtitle("Delays by Line")+
    theme_bw() %+replace% 
    theme(panel.background = element_rect(fill = NA),
          panel.grid.major.x=element_blank(),
          panel.grid.minor.x=element_blank(),
          panel.grid.major.y=element_blank(),
          panel.grid.minor.y=element_blank(),
          axis.text.y=element_text(size=20,color="red"),
          axis.title.y=element_text(size=20),
          axis.text.x = element_text(angle = 90, hjust = 1)
    )
  #g.avg_delay
  
  DoubleAxisChart(g.count_delays,g.avg_delay)
  
}
my.df.agg = AggData(WeekDays)
HourlyDepartures(my.df.agg,WeekDays)
DeparturesByStation(my.df.agg,WeekDays,Evening.Rush.Hours,c("RICH"))
DeparturesByDestination(my.df.agg,WeekDays,Evening.Rush.Hours)
DeparturesByLine(my.df.agg,WeekDays,Evening.Rush.Hours)



