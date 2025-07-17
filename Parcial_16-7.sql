--1. Query 
/*
Mostrar código y nombre del fabricante, código y descripción del tipo de producto y la cantidad de unidades vendidas de 2 fabricantes, del que más y que menos cantidad de unidades vendieron. 
Listar solo los productos que la cantidad de unidades vendidas sea mayor a 2. No tener en cuenta aquellos fabricantes que no tuvieron ventas.
Mostrar el resultado ordenado por el código del fabricante y la cantidad de unidades vendidas por producto en forma descendente.
Nota: No se puede utilizar la cláusula WITH.
*/
select m.manu_code, m.manu_name, i.stock_num, pt.description, sum(i.quantity) cantVendida
from manufact m join items i on (i.manu_code = m.manu_code)
                join product_types pt on (pt.stock_num = i.stock_num)
where m.manu_code in ((select top 1 i2.manu_code
                        from items i2
                        group by manu_code
                        order by sum(i2.quantity) desc),
                        (select top 1 i3.manu_code
                        from items i3
                        group by manu_code
                        order by sum(i3.quantity) asc))
group by m.manu_code, m.manu_name, i.stock_num, pt.description
having sum(i.quantity) > 2
order by m.manu_code desc, cantVendida desc


--2 Procedure
/*
Crear un procedimiento actualizaClientePR el cuál tomará de una tabla "NovedadesClientes" la siguiente información:
Customer_num, lname, fname, Company

Por cada fila de la tabla NovedadesClientes se deberá evaluar:

Si el cliente existe en la tabla Customer, se deberá modificar dicho cliente en la tabla Customer con los datos leídos de la tabla NovedadesClientes.

Si el cliente no existe, se deberá insertar el cliente en la tabla Customer con los datos leídos de la tabla NovedadesClientes.

Además, el procedimiento deberá almacenar por cada una de las operaciones realizadas, una fila en una tabla Auditoría con los siguientes atributos:

IdAuditoría (Identity), operación (I ó M), customer_num, lname, fname

Ante cualquier error, informarlo y seguir procesando las novedades (Manejar UNA transacción por cada novedad).

*/
--drop table auditoria
create table auditoria(
    idAuditoria INT IDENTITY(1,1),
    operacion char(1) CHECK (operacion IN ('I', 'M')),
    customer_num smallint,
    lname VARCHAR(15),
    fname VARCHAR(15),
    company VARCHAR(20)
)
--drop table novedadesClientes
create table novedadesClientes (customer_num smallint, 
            lname VARCHAR(15), 
            fname VARCHAR(15),
            company VARCHAR(20)
)

-- drop procedure actualizaClientePR 
create procedure actualizaClientePR 
AS
BEGIN

    
    DECLARE clientes_nuevos_Cursor CURSOR FOR 
            select customer_num, lname, fname, company 
            from novedadesClientes

    DECLARE @customer_num smallint, 
            @lname VARCHAR(15), 
            @fname VARCHAR(15),
            @company VARCHAR(20)

    --
    OPEN clientes_nuevos_Cursor

    FETCH NEXT FROM clientes_nuevos_Cursor into @customer_num, @lname, 
            @fname, @company
--
WHILE @@FETCH_STATUS = 0
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION

            if exists (select 1 from customer where customer_num = @customer_num)
            BEGIN
                update customer 
                set lname = @lname, fname = @fname, company = @company 
                where customer_num = @customer_num

                insert into auditoria (operacion, customer_num, lname, fname)
                values ('M', @customer_num,@lname, @fname)
            END
            else 
            BEGIN
                insert into customer (customer_num, lname, fname, company)
                values(@customer_num,@lname, @fname, @company)

                insert into auditoria (operacion, customer_num, lname, fname)
                            values ('I', @customer_num,@lname, @fname)
            END
        COMMIT TRANSACTION
    END TRY

    BEGIN CATCH
        DECLARE @error_message char(100)
        set @error_message = ERROR_MESSAGE()
        print 'Error procesando el cliente ' + CAST(@customer_num as varchar)
        ROLLBACK TRANSACTION
    END CATCH

    FETCH NEXT FROM clientes_nuevos_Cursor into @customer_num, @lname, @fname, @company

END

CLOSE clientes_nuevos_Cursor
DEALLOCATE clientes_nuevos_Cursor

END 

--3 Trigger
/*
Crear una tabla PermisosxProducto que contiene por cada customer_num los productos que este cliente puede comprar.
La estructura de la tabla es la siguiente:

(Customer_num smallint, Manu_code char(3), Stock_num smallint)

Se pide crear un trigger que, ante la inserción de una o varias filas en la tabla items, valide que el customer_num de la orden a la que pertenece cada ítem tenga permiso de compra sobre el producto asociado a dicho ítem (manu_code, stock_num).
En caso que el cliente (customer_num) no tenga permisos (no exista un registro en la tabla permisosPorProducto) no deberá permitir la inserción, enviando un mensaje de error y deshacer todas las operaciones realizadas.

Nota: Las inserciones pueden ser masivas y de órdenes de varios clientes. Si algún cliente no cumple con la condición se deben cancelar todas las operaciones.
*/

-- Creación de tabla
CREATE TABLE permisosXproducto
(
    customer_num smallint,
    manu_code char(3),
    stock_num smallint
)


--  Versión 1: Con cursores
--drop TRIGGER permisoCliente
CREATE TRIGGER permisoCliente
ON items
AFTER INSERT
AS
BEGIN
    declare items_cursor CURSOR FOR 
    select i.item_num, i.order_num, i.quantity, i.stock_num, i.manu_code, 
    i.unit_price
    from inserted i 

    declare @item_num smallint, @order_num smallint, 
    @quantity smallint, @stock_num smallint, @manu_code char(3), 
    @unit_price decimal(8, 2), @customer_num smallint
    
open items_cursor
FETCH NEXT FROM items_cursor INTO @item_num, @order_num, @quantity,
                @stock_num, @manu_code, @unit_price

while @@FETCH_STATUS = 0
begin
    select @customer_num = o.customer_num 
    from orders o
    where o.order_num = @order_num
    --
    IF NOT EXISTS (select 1 from permisosXproducto p 
              where p.customer_num = @customer_num 
                    AND p.manu_code = @manu_code
                    AND p.stock_num = @stock_num)
    BEGIN
        declare @errorMessage char(100)
        set @errorMessage = 'El cliente ' + CAST(@customer_num as varchar) + ' no tiene permisos
        para comprar este producto';
        
        throw 50000, @errorMessage, 1 
    END

FETCH NEXT FROM items_cursor INTO @item_num, @order_num, @quantity,
                @stock_num, @manu_code, @unit_price
end

close items_cursor
DEALLOCATE items_cursor
END


-- Versión 2: Sin cursores
CREATE TRIGGER permisoCliente
ON items
AFTER INSERT
AS
BEGIN
    IF EXISTS (
        SELECT 1
        FROM inserted i
        JOIN orders o ON i.order_num = o.order_num LEFT JOIN permisosXproducto p
            ON (p.customer_num = o.customer_num 
            AND p.manu_code = i.manu_code 
            AND p.stock_num = i.stock_num)
        where p.customer_num IS NULL
    )
    BEGIN
        declare @errorMessage char(100)
        set @errorMessage = 'El cliente ' + CAST(@customer_num as varchar) + ' no tiene permisos para comprar este producto';
        throw 50000, @errorMessage, 1 
    END
END;
