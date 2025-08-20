Table DIM_PRODUCT {
  ce_brand_flvr integer [primary key]
  brand_nm varchar
}

Table DIM_PACKAGE {
  id integer [primary key]
  pkg_cat varchar
  pkg_cat_desc varchar
  tsr_pckg_nm varchar
}

Table DIM_CHANNEL_GROUP {
  id integer [primary key]
  trade_chnl_desc varchar
  trade_group_desc varchar
  trade_type_desc varchar
}

Table SALES {
  id integer [primary key]
  date date
  btlr_org_lvl_c_desc varchar
  ce_brand_flvr varchar
  brand_nm varchar
  channel_group_id integer
  id_package integer
  volume float
  year integer
  month integer
  period integer
}


Ref prod_sales: SALES.ce_brand_flvr > DIM_PRODUCT.ce_brand_flvr
Ref prod_sales: SALES.brand_nm > DIM_PRODUCT.brand_nm

Ref package_sales: SALES.id_package > DIM_PACKAGE.id

Ref channel_group_sales : SALES.channel_group_id > DIM_CHANNEL_GROUP.id