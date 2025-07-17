/*
1. Listar Número de Cliente, apellido y nombre, Total Comprado por el cliente ‘Total del Cliente’,
Cantidad de Órdenes de Compra del cliente ‘OCs del Cliente’ y la Cant. de Órdenes de Compra
solicitadas por todos los clientes ‘Cant. Total OC’, de todos aquellos clientes cuyo promedio de compra
por Orden supere al promedio de órdenes de compra general, tengan al menos 2 órdenes y cuyo
zipcode comience con 94.
*/

select 
  c.customer_num, 
  c.lname, 
  c.fname, 
  sum(i.quantity * i.unit_price) totalCliente,
  count(distinct o.order_num) ocCliente, 
  (select count(distinct o2.order_num) from orders o2) cantTotalOC
from customer c 
join orders o on o.customer_num = c.customer_num
join items i on i.order_num = o.order_num
where c.zipcode LIKE '94%'
group by c.customer_num, c.lname, c.fname
having count(distinct o.order_num) >= 2
   AND (sum(i.quantity*i.unit_price)/count(distinct o.order_num)) > 
       (select sum(i2.quantity*i2.unit_price) / COUNT(DISTINCT i2.order_num)
        from items i2)

/*
2. 2.a Se requiere crear una tabla temporal #ABC_Productos un ABC de Productos ordenado por cantidad
de venta en u$, los datos solicitados son:
Nro. de Stock, Código de fabricante, descripción del producto, Nombre de Fabricante, Total del producto
pedido 'u$ por Producto', Cant. de producto pedido 'Unid. por Producto', para los productos que
pertenezcan a fabricantes que fabriquen al menos 10 productos diferentes.
*/

create table #ABC_Productos (
    stock_num smallint,
    manu_code char(3),
    description varchar(15),
    manu_name varchar(15),
    totalPedidoUSS decimal(12,2),
    unidadesPorProducto int
)

insert into #ABC_Productos
select i.stock_num, i.manu_code, pt.description, m.manu_name,
       sum(i.quantity*i.unit_price) totalPedidoUSS,
       sum(i.quantity) unidadesPorProducto
from items i 
join product_types pt on (pt.stock_num=i.stock_num)
join manufact m on (m.manu_code = i.manu_code)
where m.manu_code IN (select p2.manu_code from products p2 
                      group by p2.manu_code 
                      having count(p2.stock_num) >= 10)
group by i.stock_num, i.manu_code, pt.description, m.manu_name
order by totalPedidoUSS DESC;


/*
3. En función a la tabla temporal generada en el punto 2, obtener un listado que detalle para cada tipo
de producto existente en #ABC_Producto, la descripción del producto, el mes en el que fue solicitado, el
cliente que lo solicitó (en formato 'Apellido, Nombre'), la cantidad de órdenes de compra 'Cant OC por
mes', la cantidad del producto solicitado 'Unid Producto por mes' y el total en u$ solicitado 'u$ Producto
por mes'.
Mostrar sólo aquellos clientes que vivan en el estado con mayor cantidad de clientes, ordenado por
mes y descripción del tipo de producto en forma ascendente y por cantidad de productos por mes en
forma descendente.
*/



select c.customer_num, c.lname, i.manu_code, i.stock_num,
    SUM(i.quantity) AS cantidad
from customer c
    join orders o ON o.customer_num = c.customer_num
    join items i ON i.order_num = o.order_num
where i.manu_code IN ('HSK', 'NRG')
    AND NOT EXISTS (
        select stock_num
        from products p
        where p.manu_code IN ('HSK', 'NRG')

        EXCEPT

        select DISTINCT i2.stock_num
        from items i2
            join orders o2 ON i2.order_num = o2.order_num
        where 
            o2.customer_num = c.customer_num 
            AND i2.manu_code IN ('HSK', 'NRG')
    )
group by 
    c.customer_num, c.lname, i.manu_code, i.stock_num
order by 
    c.customer_num, cantidad DESC;
