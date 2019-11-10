package db

case class DolarInfo(id: Int, fecha: String, open: Float, high: Float, low: Float, last: Float, cierre: Float,
                     aj_dif: Float, mon: String, ol_vol: Int, ol_dif: Int, vol_ope: Int, unidad: String,
                     dolar_bn: Float, dolar_itau: Option[Float], dif_sem: Float)
