/*
1. Dada la tabla Products de la base de datos stores7 se requiere crear una tabla
Products_historia_precios y crear un trigger que registre los cambios de precios que se hayan
producido en la tabla Products.
Tabla Products_historia_precios
 Stock_historia_Id Identity (PK)
 Stock_num
 Manu_code
 fechaHora (grabar fecha y hora del evento)
 usuario (grabar usuario que realiza el cambio de precios)
 unit_price_old
 unit_price_new
 estado char default ‘A’ check (estado IN (‘A’,’I’)
*/
--1 
create table Products_historia_precios(
    stock_historia_Id int IDENTITY(1,1) PRIMARY KEY,
    stock_num smallint,
    manu_code char(3),
    fechaHora DATETIME DEFAULT GETDATE(),
    usuario varchar(20) DEFAULT CURRENT_USER,
    unit_price_old decimal(6, 2),
    unit_price_new decimal(6, 2),
    estado char(1) DEFAULT 'A'  CHECK (estado IN ('A', 'I'))
)

GO
create trigger t_historia_precios 
on 
products
after UPDATE
AS
BEGIN
    declare @stock_num smallint, @manu_code char(3), @unit_price_old decimal(6, 2),
            @unit_price_new decimal(6, 2)

    declare cursor_products CURSOR FOR 
        select  i.stock_num, i.manu_code, i.unit_price,
        d.unit_price  from inserted i join deleted d 
        on (i.stock_num = d.stock_num AND i.manu_code = d.manu_code)
        where i.unit_price != d.unit_price
    
    open cursor_products
    fetch next from cursor_products into @stock_num,  @manu_code, @unit_price_new, 
                                         @unit_price_old
    while @@FETCH_STATUS = 0
    BEGIN
        insert into Products_historia_precios (stock_num,  manu_code, unit_price_old, 
                                              unit_price_new,fechaHora, usuario )
        values (@stock_num,  @manu_code, @unit_price_old, @unit_price_new,
                GETDATE(), SYSTEM_USER)

       
    fetch next from cursor_products into @stock_num,  @manu_code, @unit_price_new, 
                                         @unit_price_old
    end


    close cursor_products
    DEALLOCATE cursor_products

END

/*
2.Crear un trigger sobre la tabla Products_historia_precios que ante un delete sobre la misma
realice en su lugar un update del campo estado de ‘A’ a ‘I’ (inactivo).
*/

CREATE TRIGGER productsBorrado 
ON products_historia_precios
INSTEAD OF DELETE
AS
BEGIN
    DECLARE @stock_historia_id INT

    DECLARE borrados_cursor CURSOR FOR 
        SELECT stock_historia_Id FROM deleted

    OPEN borrados_cursor
    FETCH NEXT FROM borrados_cursor INTO @stock_historia_id

    WHILE @@FETCH_STATUS = 0
    BEGIN
        UPDATE products_historia_precios 
        SET estado = 'I' 
        WHERE stock_historia_id = @stock_historia_id 

        FETCH NEXT FROM borrados_cursor INTO @stock_historia_id
    END

    CLOSE borrados_cursor
    DEALLOCATE borrados_cursor
END


/*
3. Crear la vista Productos_x_fabricante que tenga los siguientes atributos:
Stock_num, description, manu_code, manu_name, unit_price
Crear un trigger de Insert sobre la vista anterior que ante un insert, inserte una fila en la tabla
Products, pero si el manu_code no existe en la tabla manufact, inserte además una fila en dicha
tabla con el campo lead_time en 1.
*/
create view productos_x_fabricante (
    Stock_num, description, manu_code, manu_name, unit_price
) AS 
select p.stock_num, pt.description, p.manu_code, m.manu_name, p.unit_price
from products p 
join product_types pt on (p.stock_num = pt.stock_num)
join manufact m on (p.manu_code = m.manu_code)


create trigger insert_productos_x_fabricante_TR 
on productos_x_fabricante
instead of INSERT
AS
BEGIN
    declare @stock_num smallint, @description varchar(15), 
    @manu_code char(3), @manu_name varchar(15), @unit_price decimal(6,2)

    declare producto_nuevo_cursor CURSOR for select i.stock_num, i.description, i.manu_code, i.manu_name, i.unit_price
                                             from inserted i
    open producto_nuevo_cursor

    fetch next from producto_nuevo_cursor into @stock_num, @description, @manu_code, @manu_name, @unit_price

    while @@FETCH_STATUS = 0
    begin 
    -- hay que chequear primero el manu_code porq si 
    --if @manu_code NOT IN (select manu_code from manufact)
    IF NOT EXISTS (SELECT 1 FROM manufact WHERE manu_code = @manu_code)
    insert into manufact(manu_code, manu_name, lead_time) values (@manu_code, @manu_name, 1)

    insert into products (stock_num, manu_code, unit_price) values (@stock_num, @manu_code, @unit_price)


    fetch next from producto_nuevo_cursor into @stock_num, @description, @manu_code, @manu_name, @unit_price
    end
    close producto_nuevo_cursor
    DEALLOCATE producto_nuevo_cursor
END
