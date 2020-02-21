

-- switch(
    select 1 as R1
-- ) {

    -- case (1)
        -- for(i, 12, 24, 2) {

            select filename from table_files where fileid = variable(i)
        -- }
    -- case (2)
        -- if (

            select true as OK
        -- )

        -- {
            select * from tableS

        -- }
		-- else {
			select * from tableElse
		-- }
    -- default
        ---- while (
            -- comment
            -- {
                with
                cte as (select * from tableA)
                select * from cte
            -- }
            select true as OK
        -- )
		select x,y,z from coordinates
-- }

-- {