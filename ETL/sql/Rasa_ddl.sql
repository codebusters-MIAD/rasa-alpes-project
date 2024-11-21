DROP TABLE IF EXISTS Rs_HechoPlanesTiposBeneficio;
DROP TABLE IF EXISTS Rs_HechoHistCondicionesTiposBeneficio;
DROP TABLE IF EXISTS Rs_AsociacionAreaServicioGeografia;
DROP TABLE IF EXISTS Rs_CondicionDePago;
DROP TABLE IF EXISTS Rs_NivelesDeServicio;
DROP TABLE IF EXISTS Rs_Geografia;
DROP TABLE IF EXISTS Rs_AreasDeServicio;
DROP TABLE IF EXISTS Rs_Proveedor;
DROP TABLE IF EXISTS Rs_TiposBeneficio;
DROP TABLE IF EXISTS Rs_MiniCondicionesTipoBeneficio;
DROP TABLE IF EXISTS Rs_Fecha;

CREATE TABLE Rs_MiniCondicionesTipoBeneficio
(
    IdCondicionesBeneficios_DWH              INT PRIMARY KEY,
    IdTipoBeneficio_T                        INT NOT NULL,
    Annio                                    INT NOT NULL,
    EstaCubiertoPorSeguro                    NVARCHAR(3) CHECK (EstaCubiertoPorSeguro IN ('Yes', 'No')),
    EsEHB                                    NVARCHAR(3) CHECK (EsEHB IN ('Yes', 'No')),
    TieneLimiteCuantitativo                  NVARCHAR(3) CHECK (TieneLimiteCuantitativo IN ('Yes', 'No')),
    ExcluidoDelDesembolsoMaximoDentroDeLaRed NVARCHAR(3) CHECK (ExcluidoDelDesembolsoMaximoDentroDeLaRed IN ('Yes', 'No')),
    ExcluidoDelDesembolsoMaximoFueraDeLaRed  NVARCHAR(3) CHECK (ExcluidoDelDesembolsoMaximoFueraDeLaRed IN ('Yes', 'No')),
    UnidadDelLimite                          NVARCHAR(50)
);

CREATE TABLE Rs_Proveedor
(
    IdProveedor_DWH INT PRIMARY KEY,
    IdProveedor_T   INT NOT NULL
);

CREATE TABLE Rs_TiposBeneficio
(
    IdTipoBeneficio_DWH INT PRIMARY KEY,
    IdTipoBeneficio_T   INT NOT NULL,
    Nombre              NVARCHAR(100) NOT NULL
);


CREATE TABLE Rs_Fecha_T
(
    IdFecha INT PRIMARY KEY,
    Fecha   DATE NOT NULL,
    Annio    INT NOT NULL,
    Mes     INT NOT NULL,
    Dia     INT NOT NULL
);

CREATE TABLE Rs_HechoHistCondicionesTiposBeneficio
(
    IdTipoBeneficio_DWH            INT NOT NULL,
    IdCondicionesTipoBeneficio_DWH INT NOT NULL,
    IdFechaInicio                  INT NOT NULL,
    IdFechaFin                     INT NOT NULL,
    Cambio                         NVARCHAR(3) CHECK (Cambio IN ('Yes', 'No')) NOT NULL, -- Actual que vimos en la explicacion
    PRIMARY KEY (IdTipoBeneficio_DWH, IdCondicionesTipoBeneficio_DWH, IdFechaInicio, IdFechaFin),
    FOREIGN KEY (IdTipoBeneficio_DWH) REFERENCES Rs_TiposBeneficio (IdTipoBeneficio_DWH),
    FOREIGN KEY (IdCondicionesTipoBeneficio_DWH) REFERENCES Rs_MiniCondicionesTipoBeneficio (IdCondicionesBeneficios_DWH),
    FOREIGN KEY (IdFechaInicio) REFERENCES Rs_Fecha (IdFecha),
    FOREIGN KEY (IdFechaFin) REFERENCES Rs_Fecha (IdFecha)
);

CREATE TABLE Rs_AreasDeServicio
(
    IdAreaDeServicio_DWH INT PRIMARY KEY,
    IdAreaDeServicio_T   INT NOT NULL,
    Nombre               NVARCHAR(100) NOT NULL,
    AnnioCreacion         DATE NOT NULL
);

CREATE TABLE Rs_Geografia
(
    IdGeografia_DWH INT PRIMARY KEY,
    IdGeografia_T   INT NOT NULL,
    Estado          NVARCHAR(50) NOT NULL,
    Condado         NVARCHAR(50) NOT NULL,
    AreaAct         DECIMAL(18, 2) NOT NULL,
    DensidadAct     DECIMAL(18, 2) NOT NULL,
    PoblacionAct    INT NOT NULL
);


CREATE TABLE Rs_AsociacionAreaServicioGeografia
(
    IdAreaDeServicio_DWH INT NOT NULL,
    IdGeografia_DWH      INT NOT NULL,
    PRIMARY KEY (IdAreaDeServicio_DWH, IdGeografia_DWH),
    FOREIGN KEY (IdAreaDeServicio_DWH) REFERENCES Rs_AreasDeServicio (IdAreaDeServicio_DWH),
    FOREIGN KEY (IdGeografia_DWH) REFERENCES Rs_Geografia (IdGeografia_DWH)
);

CREATE TABLE Rs_NivelesDeServicio
(
    IdNivelDeServicio_DWH INT PRIMARY KEY,
    IdNivelDeServicio_T   INT NOT NULL,
    Descripcion           NVARCHAR(200)
);

CREATE TABLE Rs_CondicionDePago
(
    IdCondicionDePago_DWH INT PRIMARY KEY,
    IdCondicionDePago_T   INT NOT NULL,
    Descripcion           NVARCHAR(200),
    Tipo                  NVARCHAR(50) NOT NULL
);

CREATE TABLE Rs_HechoPlanesTiposBeneficio
(
    IdProveedor_DWH                INT NOT NULL,
    IdFechaEmision                 INT NOT NULL,
    IdPlan_DD                      NVARCHAR(100) NOT NULL,
    IdAreaDeServicio_DWH           INT NOT NULL,
    IdNivelDeServicio_DWH          INT NOT NULL,
    IdTipoBeneficio_DWH            INT NOT NULL,
    IdCondicionesTipoBeneficio_DWH INT NOT NULL,
    IdCondicionDePagoCoseguro_DWH  INT NOT NULL,
    IdCondicionDePagoCopago_DWH    INT NOT NULL,
    ValorCoseguro                  DECIMAL(18, 2),
    ValorCopago                    DECIMAL(18, 2),
    CantidadLimite                 INT,
    PRIMARY KEY (IdProveedor_DWH, IdFechaEmision, IdPlan_DD, IdAreaDeServicio_DWH, IdNivelDeServicio_DWH, IdTipoBeneficio_DWH, IdCondicionesTipoBeneficio_DWH, IdCondicionDePagoCoseguro_DWH, IdCondicionDePagoCopago_DWH),
    FOREIGN KEY (IdProveedor_DWH) REFERENCES Rs_Proveedor (IdProveedor_DWH),
    FOREIGN KEY (IdFechaEmision) REFERENCES Rs_Fecha (IdFecha),
    FOREIGN KEY (IdAreaDeServicio_DWH) REFERENCES Rs_AreasDeServicio (IdAreaDeServicio_DWH),
    FOREIGN KEY (IdNivelDeServicio_DWH) REFERENCES Rs_NivelesDeServicio (IdNivelDeServicio_DWH),
    FOREIGN KEY (IdTipoBeneficio_DWH) REFERENCES Rs_TiposBeneficio (IdTipoBeneficio_DWH),
    FOREIGN KEY (IdCondicionesTipoBeneficio_DWH) REFERENCES Rs_MiniCondicionesTipoBeneficio (IdCondicionesBeneficios_DWH),
    FOREIGN KEY (IdCondicionDePagoCoseguro_DWH) REFERENCES Rs_CondicionDePago (IdCondicionDePago_DWH),
    FOREIGN KEY (IdCondicionDePagoCopago_DWH) REFERENCES Rs_CondicionDePago (IdCondicionDePago_DWH)
);

ALTER TABLE Rs_Proveedor
    ADD INDEX idx_IdProveedor_T (IdProveedor_T);

ALTER TABLE Rs_TiposBeneficio
    ADD INDEX idx_IdTipoBeneficio_T (IdTipoBeneficio_T);

ALTER TABLE Rs_AreasDeServicio
    ADD INDEX idx_IdAreaDeServicio_T (IdAreaDeServicio_T);

ALTER TABLE Rs_Geografia
    ADD INDEX idx_IdGeografia_T (IdGeografia_T);

ALTER TABLE Rs_NivelesDeServicio
    ADD INDEX idx_IdNivelDeServicio_T (IdNivelDeServicio_T);

ALTER TABLE Rs_CondicionDePago
    ADD INDEX idx_IdCondicionDePago_T (IdCondicionDePago_T);


ALTER TABLE Rs_HechoPlanesTiposBeneficio
    ADD INDEX idx_HechoPlanesTiposBeneficio_id_plan (IdPlan_DD);

-- Cambios V1
ALTER TABLE Rs_MiniCondicionesTipoBeneficio
    ADD INDEX idx_Annio_IdTipoBeneficio_T (IdTipoBeneficio_T, Annio);
