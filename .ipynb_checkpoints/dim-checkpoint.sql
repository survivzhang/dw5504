-- Create DimTime Table
CREATE TABLE DimTime (

    TimeDimensionIndex INT PRIMARY KEY,        -- 时间维度的唯一索引
    Month INT NOT NULL,                        -- 月份
    Year INT NOT NULL,                         -- 年
    Dayweek VARCHAR(50) NOT NULL,              -- 周中的天
    Time VARCHAR(50) NOT NULL,                 -- 时间
    DayOfWeek VARCHAR(50) NOT NULL,            -- 星期几
    TimeOfDay VARCHAR(50) NOT NULL             -- 日间时段（如早晨、下午等）
);

-- Create DimLocation Table
CREATE TABLE DimLocation (
    LocationDimensionIndex INT PRIMARY KEY,    -- 地点维度的唯一索引
    NationalLGAName VARCHAR(255) NOT NULL,     -- 国家地方政府区域名
    State VARCHAR(50) NOT NULL,                -- 所在州
    NationalRemotenessAreas VARCHAR(50) NULL,  -- 国家远离地区
    SA4Name VARCHAR(255) NOT NULL             -- SA4区域名
);

-- Create DimPersonnel Table
CREATE TABLE DimPersonnel (
    PersonnelDimensionIndex INT PRIMARY KEY,   -- 人员维度的唯一索引
    CrashID VARCHAR(50) NOT NULL,           -- 事故 ID
    Gender VARCHAR(50) NOT NULL,               -- 性别
    AgeGroup VARCHAR(50) NOT NULL,             -- 年龄组
    RoadUser VARCHAR(50) NOT NULL              -- 道路使用者类型
);

-- Create DimVehicleType Table
CREATE TABLE DimVehicleType (
    VehicleTypeDimensionIndex INT PRIMARY KEY, -- 车辆类型维度的唯一索引
    BusInvolvement VARCHAR(50) NOT NULL,           -- 公交车涉事
    HeavyRigidTruckInvolvement VARCHAR(50) NOT NULL, -- 重型刚性卡车涉事
    ArticulatedTruckInvolvement VARCHAR(50) NOT NULL -- 关节卡车涉事
);

-- Create DimRoadType Table
CREATE TABLE DimRoadType (
    RoadTypeDimensionIndex INT PRIMARY KEY,    -- 道路类型维度的唯一索引
    NationalRoadType VARCHAR(50) NOT NULL      -- 国家道路类型
);

-- Create DimAccidentInfo Table
CREATE TABLE DimAccidentInfo (
    AccidentTypeDimensionIndex INT PRIMARY KEY, -- 事故类型维度的唯一索引
    CrashType VARCHAR(100) NOT NULL            -- 事故类型
);

-- Create DimNumberFatalities Table
CREATE TABLE DimNumberFatalities (
    NumberFatalities INT PRIMARY KEY    -- 死亡人数作为主键
);

-- Create DimSpecialPeriod Table
CREATE TABLE DimSpecialPeriod (
    SpecialPeriodDimensionIndex INT PRIMARY KEY, -- 特殊节假日维度的唯一索引
    ChristmasPeriod VARCHAR(50) NOT NULL,            -- 是否是圣诞节期间
    EasterPeriod VARCHAR(50) NOT NULL                -- 是否是复活节期间
);

-- Create DimDwelling Table
CREATE TABLE DimDwelling (
    DwellingRecords INT PRIMARY KEY     -- 住宅记录维度的唯一索引
);

-- Create DimSpeedLimit Table
CREATE TABLE DimSpeedLimit (
    SpeedLimitDimensionIndex INT PRIMARY KEY,   -- 限速维度的唯一索引
    SpeedLimit INT NOT NULL                     -- 限速（单位：km/h）
);

-- Create FactTrafficData Table
CREATE TABLE FactTrafficData (
    FactDimensionIndex INT PRIMARY KEY,          -- 主键
    CrashID VARCHAR(50) NOT NULL,                -- 事故 ID
    TimeDimensionIndex INT NOT NULL,            -- 外键，时间维度
    LocationDimensionIndex INT NOT NULL,        -- 外键，地点维度
    PersonnelDimensionIndex INT NOT NULL,       -- 外键，人员维度
    VehicleTypeDimensionIndex INT NOT NULL,     -- 外键，车辆类型维度
    RoadTypeDimensionIndex INT NOT NULL,        -- 外键，道路类型维度
    AccidentTypeDimensionIndex INT NOT NULL,    -- 外键，事故类型维度
    NumberFatalities INT,                       -- 外键，死亡人数（已通过外键定义）
    SpecialPeriodDimensionIndex INT NOT NULL,   -- 外键，特殊节假日维度
    DwellingDimensionIndex INT NOT NULL,        -- 外键，住宅记录维度
    SpeedLimitDimensionIndex INT NOT NULL,      -- 外键，限速维度
    FOREIGN KEY (TimeDimensionIndex) REFERENCES DimTime(TimeDimensionIndex),
    FOREIGN KEY (LocationDimensionIndex) REFERENCES DimLocation(LocationDimensionIndex),
    FOREIGN KEY (PersonnelDimensionIndex) REFERENCES DimPersonnel(PersonnelDimensionIndex),
    FOREIGN KEY (VehicleTypeDimensionIndex) REFERENCES DimVehicleType(VehicleTypeDimensionIndex),
    FOREIGN KEY (RoadTypeDimensionIndex) REFERENCES DimRoadType(RoadTypeDimensionIndex),
    FOREIGN KEY (AccidentTypeDimensionIndex) REFERENCES DimAccidentInfo(AccidentTypeDimensionIndex),
    FOREIGN KEY (NumberFatalities) REFERENCES DimNumberFatalities(NumberFatalities),
    FOREIGN KEY (SpecialPeriodDimensionIndex) REFERENCES DimSpecialPeriod(SpecialPeriodDimensionIndex),
    FOREIGN KEY (DwellingDimensionIndex) REFERENCES DimDwelling(DwellingRecords),  -- Corrected foreign key reference
    FOREIGN KEY (SpeedLimitDimensionIndex) REFERENCES DimSpeedLimit(SpeedLimitDimensionIndex)
);
