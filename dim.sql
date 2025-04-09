-- 创建时间维度表
CREATE TABLE Time_Dimension (
    TimeID INT PRIMARY KEY,
    Month VARCHAR(20),
    Year INT,
    Dayweek VARCHAR(20),
    Time VARCHAR(20),
    Day_of_week VARCHAR(20),
    Time_of_Day VARCHAR(20)
);

-- 创建地点维度表
CREATE TABLE Location_Dimension (
    LocationID INT PRIMARY KEY,
    National_LGA_Name_2021 VARCHAR(100),
    State VARCHAR(50),
    National_Remoteness_Areas VARCHAR(50),
    SA4_Name_2021 VARCHAR(100)
);

-- 创建人员维度表
CREATE TABLE Personnel_Dimension (
    PersonnelID INT PRIMARY KEY,
    Crash_ID INT,
    Gender VARCHAR(20),
    Age_Group VARCHAR(20),
    Road_User VARCHAR(50)
);

-- 创建车辆类型维度表
CREATE TABLE Vehicle_Type_Dimension (
    VehicleID INT PRIMARY KEY,
    Bus_Involvement VARCHAR(20),
    Heavy_Rigid_Truck_Involvement VARCHAR(20),
    Articulated_Truck_Involvement VARCHAR(20)
);

-- 创建道路类型维度表
CREATE TABLE Road_Type_Dimension (
    RoadID INT PRIMARY KEY,
    National_Road_Type VARCHAR(50)
);

-- 创建事故信息维度表
CREATE TABLE Accident_Info_Dimension (
    CrashTypeID INT PRIMARY KEY,
    Crash_Type VARCHAR(50)
);

-- 创建严重程度维度表
CREATE TABLE Fatality_Dimension (
    FatalityID INT PRIMARY KEY,
    Number_Fatalities INT
);

-- 创建特殊节假日维度表
CREATE TABLE Special_Period_Dimension (
    SpecialPeriodID INT PRIMARY KEY,
    Christmas_Period VARCHAR(20),
    Easter_Period VARCHAR(20)
);

-- 创建住宅记录维度表
CREATE TABLE Dwelling_Dimension (
    DwellingID INT PRIMARY KEY,
    Dwelling_Records VARCHAR(100)
);

-- 创建限速维度表
CREATE TABLE Speed_Limit_Dimension (
    SpeedLimitID INT PRIMARY KEY,
    Speed_Limit INT
);

-- 创建事实表
CREATE TABLE Accident_Facts (
    Accident_ID INT PRIMARY KEY,
    TimeID INT,
    LocationID INT,
    PersonnelID INT,
    VehicleID INT,
    RoadID INT,
    CrashTypeID INT,
    SpecialPeriodID INT,
    DwellingID INT,
    SpeedLimitID INT,
    FatalityID INT,
    fatalitiestotalnumber NUMERIC,
    accidenttotalnumber NUMERIC,

    FOREIGN KEY (TimeID) REFERENCES Time_Dimension(TimeID),
    FOREIGN KEY (LocationID) REFERENCES Location_Dimension(LocationID),
    FOREIGN KEY (PersonnelID) REFERENCES Personnel_Dimension(PersonnelID),
    FOREIGN KEY (VehicleID) REFERENCES Vehicle_Type_Dimension(VehicleID),
    FOREIGN KEY (RoadID) REFERENCES Road_Type_Dimension(RoadID),
    FOREIGN KEY (CrashTypeID) REFERENCES Accident_Info_Dimension(CrashTypeID),
    FOREIGN KEY (FatalityID) REFERENCES Fatality_Dimension(FatalityID),
    FOREIGN KEY (SpecialPeriodID) REFERENCES Special_Period_Dimension(SpecialPeriodID),
    FOREIGN KEY (DwellingID) REFERENCES Dwelling_Dimension(DwellingID),
    FOREIGN KEY (SpeedLimitID) REFERENCES Speed_Limit_Dimension(SpeedLimitID)
);