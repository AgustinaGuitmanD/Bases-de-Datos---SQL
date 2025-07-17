/*
Stored Procedures
1.a Crear la siguiente tabla CustomerStatistics con los siguientes campos
customer_num (entero y pk), ordersqty (entero), maxdate (date), uniqueProducts
(entero)
Crear un procedimiento ‘actualizaEstadisticas’ que reciba dos parámetros
customer_numDES y customer_numHAS y que en base a los datos de la tabla
customer cuyo customer_num estén en en rango pasado por parámetro, inserte (si
no existe) o modifique el registro de la tabla CustomerStatistics con la siguiente
información:
Ordersqty contedrá la cantidad de órdenes para cada cliente.
Maxdate contedrá la fecha máxima de la última órde puesta por cada cliente.
uniqueProducts contendrá la cantidad única de tipos de productos adquiridos
por cada cliente.
*/

CREATE TABLE CustomerStatistics (
    customer_num INT PRIMARY KEY,
    ordersqty INT,
    maxdate DATE,
    uniqueProducts INT
);
GO

CREATE PROCEDURE actualizaEstadisticas
    @customer_numDES INT,
    @customer_numHAS INT
AS
BEGIN
    DECLARE @customer_num INT,
            @ordersqty INT,
            @maxdate DATE,
            @uniqueProducts INT;

    DECLARE cursor_customer CURSOR FOR
        SELECT customer_num
        FROM customer
        WHERE customer_num BETWEEN @customer_numDES AND @customer_numHAS;

    OPEN cursor_customer;

    FETCH NEXT FROM cursor_customer INTO @customer_num;

    WHILE @@FETCH_STATUS = 0
    BEGIN
    
        SELECT 
            @ordersqty = COUNT(*),
            @maxdate = MAX(order_date)
        FROM orders
        WHERE customer_num = @customer_num;

        SELECT 
            @uniqueProducts = COUNT(DISTINCT i.stock_num)
        FROM items i
        JOIN orders o ON i.order_num = o.order_num
        WHERE o.customer_num = @customer_num;

        IF EXISTS (SELECT 1 FROM CustomerStatistics WHERE customer_num = @customer_num)
        BEGIN
            UPDATE CustomerStatistics
            SET ordersqty = @ordersqty,
                maxdate = @maxdate,
                uniqueProducts = @uniqueProducts
            WHERE customer_num = @customer_num;
        END
        ELSE
        BEGIN
            INSERT INTO CustomerStatistics (customer_num, ordersqty, maxdate, uniqueProducts)
            VALUES (@customer_num, @ordersqty, @maxdate, @uniqueProducts);
        END;

        FETCH NEXT FROM cursor_customer INTO @customer_num;
    END

    CLOSE cursor_customer;
    DEALLOCATE cursor_customer;
END;


/*
1.b Crear un procedimiento ‘migraClientes’ que reciba dos parámetros
customer_numDES y customer_numHAS y que dependiendo el tipo de cliente y la
cantidad de órdenes los inserte en las tablas clientesCalifornia, clientesNoCaBaja,
clienteNoCAAlta.

• El procedimiento deberá migrar de la tabla customer todos los
clientes de California a la tabla clientesCalifornia, los clientes que no
son de California pero tienen más de 999u$ en OC en
clientesNoCaAlta y los clientes que tiene menos de 1000u$ en OC en
la tablas clientesNoCaBaja.
• Se deberá actualizar un campo status en la tabla customer con valor
‘P’ Procesado, para todos aquellos clientes migrados.
• El procedimiento deberá contemplar toda la migración como un lote,
en el caso que ocurra un error, se deberá informar el error ocurrido y
abortar y deshacer la operación.
*/

ALTER TABLE customer ADD status CHAR(1)


create procedure migraClientes @customer_numDES int, 
                                @customer_numHAS int
AS
BEGIN
begin TRY
declare @customer_num smallint,@fname varchar(15), 
        @lname varchar(15) , @company varchar(20),
        @address1 varchar(20), @address2 varchar(20),
        @city varchar(15), @state char(2) ,
        @zipcode char(5), @phone varchar(18),
        @status char(1)

declare cursor_customer_num cursor for 
    select customer_num, fname, lname, company, address1, 
           address2, city, state, zipcode, phone
    from customer
    where customer_num between @customer_numDES and @customer_numHAS

open cursor_customer_num

fetch next from cursor_customer_num into @customer_num, @fname,  @lname, 
                                         @company, @address1 , @address2,
                                         @city,@state, @zipcode, @phone
begin transaction

    while @@FETCH_STATUS = 0
    BEGIN

    if @state = 'CA' 
    insert into clientesCalifornia (customer_num, fname, lname, company, 
                                    address1 , address2, city,
                                    state, zipcode, phone)
                values (@customer_num, @fname,  @lname, @company, 
                        @address1 , @address2,@city,
                        @state, @zipcode, @phone)

    else 
    begin
        if (select sum(i.unit_price * i.quantity) 
            from items i join orders o on (o.order_num = i.order_num)
            where o.customer_num = @customer_num)  > 999
        begin 
            insert into clientesNoCaAlta values (@customer_num, @fname, @lname, 
                                                @company, @address1 , @address2,
                                                @city, @state, @zipcode, @phone)
        end
        else
        begin
            insert into clientesNoCaBaja values (@customer_num, @fname, @lname, 
                                                 @company, @address1 , @address2,
                                                 @city, @state, @zipcode, @phone)
        end
    END
    
    update customer set status = 'P' 
                    where customer_num = @customer_num

    fetch next from cursor_customer_num into @customer_num, @fname,  @lname, 
                                             @company, @address1, @address2, 
                                             @city,@state, @zipcode, @phone
    END;
---
    commit transaction
    close cursor_customer_num
    deallocate cursor_customer_num
end TRY
--
begin catch
    close cursor_customer_num
    deallocate cursor_customer_num
    ROLLBACK TRANSACTION
    declare @error_message varchar(100) 
    select @error_message = 'Error en cliente ' + cast(@customer_num AS char(5));
    THROW 500000, @error_message, 1
end catch

END

drop procedure migraClientes


/*
1.c Crear un procedimiento ‘actualizaPrecios’ que reciba como parámetros
manu_codeDES, manu_codeHAS y porcActualizacion que dependiendo del tipo de
cliente y la cantidad de órdenes genere las siguientes tablas listaPrecioMayor y
listaPreciosMenor. Ambas tienen las misma estructura que la tabla Productos.
• El procedimiento deberá tomar de la tabla Productos todos los productos que
correspondan al rango de fabricantes asignados por parámetro.
Por cada producto del fabricante se evaluará la cantidad (quantity) comprada.
Si la misma es mayor o igual a 500 se grabará el producto en la tabla
listaPrecioMayor y el unit_price deberá ser actualizado con (unit_price *
(porcActualización *0,80)),
Si la cantidad comprada del producto es menor a 500 se actualizará (o insertará)
en la tabla listaPrecioMenor y el unit_price se actualizará con (unit_price *
porcActualizacion)
• Asimismo, se deberá actualizar un campo status de la tabla stock con valor ‘A’
Actualizado, para todos aquellos productos con cambio de precio actualizado.

• El procedimiento deberá contemplar todas las operaciones de cada fabricante
como un lote, en el caso que ocurra un error, se deberá informar el error ocurrido
y deshacer la operación de ese fabricante.
      */

CREATE TABLE listaPrecioMenor
(
    stock_num smallint NOT NULL,
    manu_code char(3) NOT NULL,
    unit_price decimal(6, 2) NULL,
    unit_code smallint NULL
)
CREATE TABLE listaPrecioMayor
(
    stock_num smallint NOT NULL,
    manu_code char(3) NOT NULL,
    unit_price decimal(6, 2) NULL,
    unit_code smallint NULL
)
alter table products add status char(1) 

GO
create procedure actualizaPrecios @manu_codeDES CHAR(3),
                                  @manu_codeHAS CHAR(3),
                                  @porcActualizacion decimal (5,3)  
AS
BEGIN
    DECLARE cursor_products CURSOR FOR 
                        select manu_code, stock_num, unit_price, unit_code
                        from products
                        where manu_code between @manu_codeDES and @manu_codeHAS
    
DECLARE @manu_code char(3), @stock_num smallint, @unit_price decimal(6, 2), 
        @unit_code smallint, @manu_codeAux CHAR(3)

open cursor_products

FETCH NEXT FROM cursor_products into @manu_code, @stock_num, 
                                    @unit_price, @unit_code
set @manu_codeAux = @manu_code

WHILE @@FETCH_STATUS = 0
BEGIN
    begin try 
    begin TRANSACTION
        if (select sum(i.quantity) from items i 
            where i.manu_code = @manu_code and stock_num = @stock_num ) >=500
            BEGIN
                insert into listaPrecioMayor (manu_code, stock_num, unit_price, unit_code) 
                values (@manu_code, @stock_num, @unit_price * (1 + @porcActualizacion * 0.80), @unit_code)
            END
        else 
            BEGIN
                insert into listaPrecioMenor (manu_code, stock_num, unit_price, unit_code)
                values (@manu_code, @stock_num, @unit_price * (1 + @porcActualizacion), @unit_code)
            END
        update products SET status = 'A' 
        where manu_code = @manu_code and stock_num = @stock_num

    FETCH NEXT FROM cursor_products into @manu_code, @stock_num, @unit_price, @unit_code
    IF @manu_code != @manu_codeAux
    BEGIN
        commit transaction
        SET @manu_codeAux = @manu_code
    END     
    end TRY
    --
    begin catch
        ROLLBACK TRANSACTION
        declare @error_message char(100)
        select @error_message = 'Error con el fabricante ' + @manu_code;
        THROW 50000, @error_message, 1
    end catch
END

END;
CLOSE cursor_products
DEALLOCATE cursor_products

end

