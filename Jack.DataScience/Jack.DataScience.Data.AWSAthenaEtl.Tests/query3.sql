-- #if blocks

select 
-- comment x
20 as value, 
-- comment 
'test' as ID

-- if (

    select true as OK
-- )

-- {
    select * from tableS

-- }


-- if (

    select true as OK
-- )

-- {
    select * from tableS

-- }

-- elseif (
select false as OK
-- )

-- {
    select * from tableS

-- }